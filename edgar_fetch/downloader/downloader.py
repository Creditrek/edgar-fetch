import os
import io
import datetime
import zipfile
import tempfile
import logging
import sys
import multiprocessing
import shutil

import requests


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
    logging.info(f"Downloading SEC filings since {since}.")
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
        logging.info(f"Skipping {dest2}")
        return

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
                    logging.info(f"Downloaded {url} to {dest1}{dest2}")
    else:
        raise logging.error("Please note edgar-fetch currently only supports zipped index files.")


class Fetcher:
    def __init__(self, destination, since, is_all_present_except_last_skipped):
        self.destination = destination
        self. since = since
        self.is_all_present_except_last_skipped = False

    # There will be only accessors and no mutators
    def get_all(self):
        """
        A method to download all files at once. 
        """
        if not os.path.exists(self.destination):
            os.makedirs(self.destination)

        tasks = _generate_quarterly_idex_list(self.since)
        logging.info("%d index files to retrieve", len(tasks))

        worker_count = _count_worker()
        logging.info(f"Number of workers running in parallel: {worker_count}")
        pool = multiprocessing.Pool(worker_count)

        for i, file in enumerate(tasks):
            is_file_skipped = self.is_all_present_except_last_skipped
            if i == 0:
                # The first item should always be re-downloaded.
                is_file_skipped = False
            pool.apply_async(_download, (file, self.destination, is_file_skipped))

        pool.close()
        pool.join()
        logging.info("Download of all SEC filings complete.")

    def get_indexed(self):
        pass
