import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List, Set
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup

URL = "http://200.152.38.155/CNPJ/"
DOWNLOAD_FOLDER = "./zip"


def get_last_modified_date() -> str:
    html = requests.get(URL).text
    soup = BeautifulSoup(html, "html.parser")

    last_modified = soup.find_all("tr")[3].find_all("td")[2].text.strip()
    last_modified = datetime.strptime(last_modified, "%Y-%m-%d %H:%M").date()

    last_modified = last_modified.strftime("%Y-%m-%d")
    return last_modified


def get_download_links() -> List[str]:
    html = requests.get(URL).text
    soup = BeautifulSoup(html, "html.parser")
    hrefs = [a.get("href") for a in soup.find_all("a")]
    urls = [URL + href for href in hrefs if href.endswith(".zip")]
    return urls


def get_dates_seen() -> Set[str]:
    already_seen = (
        subprocess.check_output("gsutil ls gs://driva-lake/crawlers/RFB".split())
        .decode("utf-8")
        .split("\n")
    )
    already_seen = [
        seen.replace("gs://driva-lake/crawlers/RFB/", "") for seen in already_seen
    ]
    filtered = set()
    for date in already_seen:
        if date:
            filtered.add(date.split("/")[0])
    return filtered


def has_new_crawl() -> bool:
    return not get_last_modified_date() in get_dates_seen()


def _download(url: str):
    process = subprocess.Popen(
        f"aria2c -c -x16 {url} -d {DOWNLOAD_FOLDER} --show-console-readout=false",
        shell=True,
        stdout=subprocess.PIPE,
    )

    while True:
        line = process.stdout.readline()
        if process.poll() is not None:
            break
        if line:
            print(line.decode("utf-8").strip())


def check_if_has_tmp() -> List[str]:
    links = []
    for file in Path(__file__).parents[1].glob("*.tmp"):
        filename = file.name
        filename = filename.replace(".tmp", "")
        links.append(f"http://200.152.38.155/CNPJ/{filename}")
    return links


def download_all(restart: bool):
    links = get_download_links()
    tmp_links = check_if_has_tmp()
    if tmp_links and restart:
        links = tmp_links
    downloaded_files = set(Path(DOWNLOAD_FOLDER).glob("*.zip"))
    if len(downloaded_files) == 37:
        print(
            f"37 arquivos já foram baixados. Apague a pasta {DOWNLOAD_FOLDER} caso queira baixar novamente."
        )
        return
    if not restart:
        shutil.rmtree(DOWNLOAD_FOLDER, ignore_errors=True)
        os.makedirs(DOWNLOAD_FOLDER)
    with ThreadPoolExecutor() as executor:
        executor.map(_download, links)

    if len(list(Path(DOWNLOAD_FOLDER).glob("*.zip"))) != len(links):
        raise ValueError("Não foi possível baixar todos os arquivos")
