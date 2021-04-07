import os
import io
import datetime
import zipfile
import tempfile
# import logging
import sys
import multiprocessing
import shutil
from typing import ClassVar, List

import requests

from .constants import DATE_FORMAT_TOKENS, DEFAULT_AFTER_DATE, DEFAULT_BEFORE_DATE
from .constants import SUPPORTED_FILINGS as _SUPPORTED_FILINGS
from .utils import (download_filings, 
    get_filing_urls_to_download, 
    get_number_of_unique_filings,
    validate_date_format)


EDGAR_PREFIX = "https://www.sec.gov/Archives/"
SEP = "|"


def _count_worker():
    count_of_cpu = 1

    try:
        count_of_cpu = len(os.sched_getaffinity(0))
    except AttributeError:
        count_of_cpu = multiprocessing.cpu_count()

    return count_of_cpu


def _get_current_quarter():
    return f"QTR{((datetime.date.today().month - 1) // 3 + 1)}s"


def _generate_quarterly_idex_list(since=1993):
    """
    Generate a list of quarterly zip files as archived in EDGAR
    since 1993 until the previous quarter.
    """
    # logging.info(f"Downloading SEC filings since {since}.")
    print(f"Downloading SEC filings since {since}.")
    years = range(since, datetime.date.today().year + 1)
    quarters = ["QTR1", "QTR2", "QTR3", "QTR4"]
    history = [(y, q) for y in years for q in quarters]
    history.reverse()

    quarter = _get_current_quarter()

    while history:
        _, q = history[0]
        if q == quarter:
            break
        else:
            history.pop(0)

    return [(
            f"{EDGAR_PREFIX} edgar/full-index/{x[0]}/{x[1]}/master.zip",
            f"{x[0]}-{x[1]}.tsv"
            ) for x in history]


def _append_txt_with_html_suffix(line):
    chunks = line.split(SEP)
    return line + SEP + chunks[-1].replace(".txt", "-index.html")


def _skip_header(file):
    for x in range(0, 11):
        file.readline()


def _request_url(url):

    with requests.get(url) as r:
        with tempfile.NamedTemporaryFile(delete=False) as t:
            shutil.copyfileobj(r, t)


def _download(file, destination, is_file_skipped):
    """
    Download an idx archive from EDGAR
    This will read idx files and unzip
    archives + read the master.idx file inside
    when skip_file is True, it will skip the file if it's already present.
    """
    if not destination.endswith("/"):
        dest1 = f"{destination}"

    url = file[0]
    dest2 = file[1]

    if is_file_skipped and os.path.exists(dest1 + dest2):
        # logging.info(f"Skipping {dest2}")
        # return
        print(f"Skipping {dest2}")
        

    if url.endswith("zip"):
        with tempfile.TemporaryFile(mode="w+b") as tmp:
            tmp.write(_request_url(url))
            with zipfile.ZipFile(tmp).open("master.idx") as z:
                with io.open(dest1 + dest2, "w+", encoding="utf-8") as idex_file:
                    _skip_header(z)
                    lines = z.read().decode("latin-1")
                    lines = map(
                        lambda line: _append_txt_with_html_suffix(line), lines.splitlines()
                    )
                    idex_file.write("\n".join(lines)+"\n")
                    # logging.info(f"Downloaded {url} to {dest1}{dest2}")
                    print(f"Downloaded {url} to {dest1}{dest2}")
    else:
        raise Exception("Please note edgar-fetch currently only supports zipped index files.")


class Fetcher:
    supported_filings: ClassVar[List[str]] = sorted(_SUPPORTED_FILINGS)

    def __init__(self):
        pass

    # There will be only accessors and no mutators
    def get_all(self, destination, since, is_all_present_except_last_skipped=False):
        """
        A method to download all files at once. 
        """
        if not os.path.exists(destination):
            os.makedirs(destination)

        files = _generate_quarterly_idex_list(since)
        # logging.info(f"A total of {len(files)} files to be retrieved.")
        print(f"A total of {len(files)} files to be retrieved.")

        worker_count = _count_worker()
        # logging.info(f"Number of workers running in parallel: {worker_count}")
        print(f"Number of workers running in parallel: {worker_count}")

        with multiprocessing.Pool(worker_count) as pool:

            for file in files:
                is_file_skipped = is_all_present_except_last_skipped
                pool.apply_async(_download, (file, destination, is_file_skipped))

        # logging.info("Download of all SEC filings complete.")
        print("Download of all SEC filings complete.")

    def get_indexed(self, destination, filing, ticker_or_cik, 
                    count_of_filings=None, after=None, before=None,
                    are_amends_included=False,
                    has_download_details=True,
                    query=""):
        """
        A companion method to download SEC filing files in batches rather than in bulk.
        """
        ticker_or_cik = str(ticker_or_cik).strip().upper()

        if not os.path.exists(destination):
            os.makedirs(destination)
        
        if count_of_filings is None:
            count_of_filings = sys.maxsize
        else:
            count_of_filings = int(count_of_filings)
            if count_of_filings < 1:
                raise ValueError(f"Invalid number encountered." 
                                 f"Please enter a value greater than or equal to 1.")

        # SEC allows for filing searches from 2000 onwards.
        if after is None:
            after = DEFAULT_AFTER_DATE.strftime(DATE_FORMAT_TOKENS)
        else:
            validate_date_format(after)

            if after < DEFAULT_AFTER_DATE.strftime(DATE_FORMAT_TOKENS):
                raise ValueError(
                    f"Filings cannot be downloaded prior to {DEFAULT_AFTER_DATE.year}. "
                    f"Please enter a date on or after {DEFAULT_AFTER_DATE}."
                )

        if before is None:
            before = DEFAULT_BEFORE_DATE.strftime(DATE_FORMAT_TOKENS)
        else:
            validate_date_format(before)

        if after > before:
            raise ValueError(
                "Invalid after and before date combination. "
                "Please enter an after date that is less than the before date."
            )

        if filing not in _SUPPORTED_FILINGS:
            filing_options = ", ".join(self.supported_filings)
            raise ValueError(
                f"{filing} filings are not supported. "
                f"Please choose from the following: {filing_options}."
            )

        if not isinstance(query, str):
            raise TypeError("Query must be of type string.")
        
        filings_to_fetch = get_filing_urls_to_download(
            filing,
            ticker_or_cik,
            count_of_filings,
            after,
            before,
            are_amends_included,
            query
        )

        download_filings(
            destination,
            ticker_or_cik,
            filing,
            filings_to_fetch,
            has_download_details
        )

        # Get the number of unique number of filings to be downloaded
        return get_number_of_unique_filings(filings_to_fetch)
