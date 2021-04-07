# Contains utility functions for the getting a subset of all available SEC filing data instead of all.

import time
from collections import namedtuple
from datetime import datetime
from pathlib import Path
from typing import List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from faker import Faker


from .constants import (DATE_FORMAT_TOKENS,
    FILING_DETAILS_FILENAME_STEM,
    FILING_FULL_SUBMISSION_FILENAME,
    ROOT_SAVE_FOLDER_NAME,
    SEC_EDGAR_ARCHIVES_BASE_URL,
    SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL,
    SEC_EDGAR_SEARCH_API_ENDPOINT)


class EdgarSearchApiError(Exception):
    """
    To throw an error when Edgar Search API encounters a problem.
    """


FilingMetadata = namedtuple(
    "FilingMetadata",
    ["accession_number", "full_submission_url", "filing_details_url", "filing_details_filename"]
)


def form_request_payload(
        ticker_or_cik,
        filing_types,
        start_date,
        end_date,
        start_index,
        query,
    ):
    payload = {
        "dateRange": "custom",
        "startdt": start_date,
        "enddt": end_date,
        "entityName": ticker_or_cik,
        "forms": filing_types,
        "from": start_index,
        "q": query,
    }
    return payload


def build_filing_metadata_from_hit(hit):
    accession_number, filing_details_filename = hit["_id"].split(":", 1)
    # The company CIK should be last in the CIK list. 
    # This list may also include the CIKs of executives 
    # carrying out insider transactions as in form 4.

    cik = hit["_source"]["ciks"][-1]
    accession_number_no_dashes = accession_number.replace("-", "", 2)

    submission_base_url = (f"{SEC_EDGAR_ARCHIVES_BASE_URL}/{cik}/{accession_number_no_dashes}")

    full_submission_url = f"{submission_base_url}/{accession_number}.txt"

    filing_details_url = f"{submission_base_url}/{filing_details_filename}"

    filing_details_filename_extension = Path(filing_details_filename).suffix.replace(
        "htm", "html"
    )
    filing_details_filename = (
        f"{FILING_DETAILS_FILENAME_STEM}{filing_details_filename_extension}"
    )

    return FilingMetadata(
        accession_number=accession_number,
        full_submission_url=full_submission_url,
        filing_details_url=filing_details_url,
        filing_details_filename=filing_details_filename,
    )


def get_filing_urls_to_download(
        filing_type,
        ticker_or_cik,
        num_filings_to_download,
        after_date,
        before_date,
        include_amends,
        query=""
    ):

    filings_to_fetch: List[FilingMetadata] = []
    start_index = 0

    while len(filings_to_fetch) < num_filings_to_download:
        payload = form_request_payload(
            ticker_or_cik, [filing_type], after_date, before_date, start_index, query
        )
        res = requests.post(
            SEC_EDGAR_SEARCH_API_ENDPOINT,
            json=payload,
            headers=get_random_user_agent_header(),
        )
        res.raise_for_status()
        search_query_results = res.json()

        if "error" in search_query_results:
            try:
                root_cause = search_query_results["error"]["root_cause"]
                if not root_cause:  # pragma: no cover
                    raise ValueError

                error_reason = root_cause[0]["reason"]
                raise EdgarSearchApiError(
                    f"Edgar Search API encountered an error: {error_reason}."
                    f"Request payload: {payload}"
                )
            except (ValueError, KeyError):  # pragma: no cover
                raise EdgarSearchApiError(
                    f"Edgar Search API encountered an unknown error."
                    f"Request payload:\n{payload}"
                )

        query_hits = search_query_results["hits"]["hits"]

        # No more results to process
        if not query_hits:
            break

        for hit in query_hits:
            hit_filing_type = hit["_source"]["file_type"]

            is_amend = hit_filing_type[-2:] == "/A"
            if not include_amends and is_amend:
                continue

            # A workaround to fix a bug where incorrect filings are sometimes included.
            # For example, AAPL 8-K searches include N-Q entries.
            if not is_amend and hit_filing_type != filing_type:
                continue

            metadata = build_filing_metadata_from_hit(hit)
            filings_to_fetch.append(metadata)

            if len(filings_to_fetch) == num_filings_to_download:
                return filings_to_fetch

        # Edgar queries 100 entries at a time, but it is best to set this
        # from the response payload in case it changes in the future
        query_size = search_query_results["query"]["size"]
        start_index += query_size

        # Prevent rate limiting
        time.sleep(SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL)

    return filings_to_fetch


faker = Faker()

def get_random_user_agent_header():
    """
    To generate a fake user-agent string to bypass the problem of SEC rate-limiting.
    """
    user_agent_chrome = faker.chrome()
    headers = {"User-Agent": user_agent_chrome}
    return headers


def resolve_relative_urls_in_filing(filing_text, base_url):

    soup = BeautifulSoup(filing_text, "lxml")

    for url in soup.find_all("a", href=True):
        url["href"] = urljoin(base_url, url["href"])

    for image in soup.find_all("img", src=True):
        image["src"] = urljoin(base_url, image["src"])

    if soup.original_encoding is None:
        return soup

    return soup.encode(soup.original_encoding)


def validate_date_format(date_format):
    error_msg_base = "Please enter a date string of the form YYYY-MM-DD."

    if not isinstance(date_format, str):
        raise TypeError(error_msg_base)

    try:
        datetime.strptime(date_format, DATE_FORMAT_TOKENS)
    except Exception:
        raise ValueError("Please input valid date(s) for bounding the time period.")
        

def download_and_save_filing(download_folder,
                             ticker_or_cik,
                             accession_number,
                             filing_type,
                             download_url,
                             save_filename,
                             resolve_urls=False,
                            ):
    res = requests.get(download_url, headers=get_random_user_agent_header())
    res.raise_for_status()
    filing_text = res.content

    # Only resolve URLs in HTML files
    if resolve_urls and Path(save_filename).suffix == ".html":
        base_url = f"{download_url.rsplit('/', 1)[0]}/"
        filing_text = resolve_relative_urls_in_filing(filing_text, base_url)

    # Create all parent directories as needed and write content to file
    save_path = (
        download_folder
        / ROOT_SAVE_FOLDER_NAME
        / ticker_or_cik
        / filing_type
        / accession_number
        / save_filename
    )
    save_path.parent.mkdir(parents=True, exist_ok=True)
    save_path.write_bytes(filing_text)

    # Prevent rate limiting when downloading
    time.sleep(SEC_EDGAR_RATE_LIMIT_SLEEP_INTERVAL)


def download_filings(
        download_folder,
        ticker_or_cik,
        filing_type,
        filings_to_fetch,
        include_filing_details,
    ):
    
    for filing in filings_to_fetch:
        try:
            download_and_save_filing(
                download_folder,
                ticker_or_cik,
                filing.accession_number,
                filing_type,
                filing.full_submission_url,
                FILING_FULL_SUBMISSION_FILENAME,
            )
        except requests.exceptions.HTTPError as e:
            print(
                "Skipping full submission download for "
                f"'{filing.accession_number}' due to network error: {e}."
            )

        if include_filing_details:
            try:
                download_and_save_filing(
                    download_folder,
                    ticker_or_cik,
                    filing.accession_number,
                    filing_type,
                    filing.filing_details_url,
                    filing.filing_details_filename,
                    resolve_urls=True,
                )
            except requests.exceptions.HTTPError as e:
                print(
                    f"Skipping SEC filing download for "
                    f"'{filing.accession_number}' due to network error: {e}."
                )


def get_number_of_unique_filings(filings):
    return len({metadata.accession_number for metadata in filings})
