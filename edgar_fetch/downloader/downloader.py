import os
import io
import urllib
import json
from pathlib import Path
import datetime
import zipfile
import tempfile
import sys
import multiprocessing
import shutil
from typing import ClassVar, List
import glob

import requests
import asyncio

from .constants import DATE_FORMAT_TOKENS, DEFAULT_AFTER_DATE, DEFAULT_BEFORE_DATE
from .constants import SUPPORTED_FILINGS as _SUPPORTED_FILINGS
from .utils import (download_filings, 
    get_filing_urls_to_download, 
    get_number_of_unique_filings,
    validate_date_format)


EDGAR_PREFIX = "https://www.sec.gov/Archives/"
SEP = "|"

DERA_URL= "https://www.sec.gov/files/dera/data/financial-statement-data-sets.html"

def _count_worker():
    count_of_cpu = 1

    try:
        count_of_cpu = len(os.sched_getaffinity(0))
    except AttributeError:
        count_of_cpu = multiprocessing.cpu_count()

    return count_of_cpu


def _get_current_quarter():
    return f"QTR{((datetime.date.today().month - 1) // 3 + 1)}"


def _generate_quarterly_idx_list(since, before):
    """
    Generate a list of quarterly zip files as archived in EDGAR
    since 1993 until the previous quarter.
    """
    # logging.info(f"Downloading SEC filings since {since}.")
    print(f"Downloading SEC filings since {since} but before {before}.")
    years = range(since, before)
    quarters = ["q1", "q2", "q3", "q4"]
    history = [(y, q) for y in years for q in quarters]
    history.reverse()

    # quarter = _get_current_quarter()

    # while history:
    #     _, q = history[0]
    #     if q == quarter:
    #         break
    #     else:
    #         history.pop(0)

    url_list = [(DERA_URL, f"/{x[0]}{x[1]}.zip") for x in history]

    return url_list


# def _append_txt_with_html_suffix(line):
#     chunks = line.split(SEP)
#     return line + SEP + chunks[-1].replace(".txt", "-index.html")


def _skip_header(file):
    for x in range(0, 11):
        file.readline()


def _request_url(url):
    with requests.get(url).text as r:
        with tempfile.NamedTemporaryFile() as fp:
            shutil.copyfileobj(r, fp)


def _download(file, data_folder, is_file_skipped):
    """
    Download an archive from DERA.
    This will read idx files and unzip archives and read the master.idx file inside
    when skip_file is True; it will skip the file if it's already present.
    """

    url_path = file[0][:-5] + file[1]
    print(url_path)

    target_directory = os.path.join(data_folder, file[1][1:-4])
    print(target_directory)

    target_file = os.path.join(data_folder, file[1][1:])
    
    # The raw files downloaded will be zipped files, hence need handling with more care. 
    res = requests.get(url_path, stream=True)
    print(res.status_code)

    handle = open(target_file, "wb")
    for chunk in res.iter_content():
        if chunk:  # To filter out keep-alive new chunks
            handle.write(chunk)
    handle.close()


class Fetcher:
    supported_filings = sorted(_SUPPORTED_FILINGS)

    def __init__(self, data_folder):
        if isinstance(data_folder, Path):
            self.data_folder = data_folder
        else:
            self.data_folder = Path(data_folder).expanduser().resolve()

    # There will be only accessors and no mutators
    async def get_all(self, since=2015, before=2021, is_all_present_except_last_skipped=False):
        """
        A method to download all files at once. 
        """

        files = _generate_quarterly_idx_list(since, before)
        print(f"A total of {len(files)} files to be retrieved.")

        worker_count = _count_worker()
        print(f"Number of workers running in parallel: {worker_count}")

        # pool = multiprocessing.Pool(worker_count)

        for file in files:
            is_file_skipped = is_all_present_except_last_skipped
            # pool.apply_async(_download, (file, self.data_folder, is_file_skipped))
            await _download(file, self.data_folder, is_file_skipped)

        # pool.close()  # reject any new tasks
        # pool.join()  # wait for the completion of all scheduled jobs

        print("Downloading of all requested SEC filings complete.")


    def unzip_files(self):
        if not str(self.data_folder).endswith("/"):
            data_folder_str = str(self.data_folder) + "/"
        else:
            data_folder_str = str(self.data_folder)
        for fullname in glob.glob(data_folder_str + "*.zip"):
            print(fullname)
            shutil.unpack_archive(fullname, fullname[:-4])

    def get_company(self, filing, ticker_or_cik, *, 
                    count_of_filings=None, after=None, before=None,
                    are_amends_included=False,
                    has_download_details=True,
                    query=""):
        """
        A companion method to download SEC filing files in batches rather than in bulk.
        """
        ticker_or_cik = str(ticker_or_cik).strip().upper()

        # if not os.path.exists(self.data_folder):
        #     os.makedirs(self.data_folder)
        
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
                f"Invalid after and before date combination.\n"
                f"Please enter an after date that is less than the before date."
            )

        if filing not in _SUPPORTED_FILINGS:
            filing_options = ", ".join(self.supported_filings)
            raise ValueError(
                f"{filing} filings are not supported.\n"
                f"Please choose from the following: {filing_options}."
            )

        if not isinstance(query, str):
            raise TypeError("Query type must be string.")
        
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
            self.data_folder,
            ticker_or_cik,
            filing,
            filings_to_fetch,
            has_download_details
        )

        # Get the number of unique number of filings to be downloaded
        return get_number_of_unique_filings(filings_to_fetch)
