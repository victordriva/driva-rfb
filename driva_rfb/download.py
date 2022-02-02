from multiprocessing import Pool
from typing import List, Set
from datetime import datetime

from pywget import wget
from bs4 import BeautifulSoup
import httpx

URL = "http://200.152.38.155/CNPJ/"
DOWNLOAD_FOLDER = "zip"


def get_last_modified_date() -> str:
    html = httpx.get(URL).text
    soup = BeautifulSoup(html, "lxml")

    last_modified = soup.find_all("tr")[3].find_all("td")[2].text.strip()
    last_modified = datetime.strptime(last_modified, "%Y-%m-%d %H:%M").date()

    last_modified = last_modified.strftime("%Y-%m-%d")
    return last_modified


def get_download_links() -> List[str]:
    html = httpx.get(URL).text
    soup = BeautifulSoup(html, "lxml")
    hrefs = [a.get("href") for a in soup.find_all("a")]
    urls = [URL + href for href in hrefs if href.endswith(".zip")]
    return urls


def get_dates_seen() -> Set[str]:
    res = httpx.get(
        "https://blob-api-uvk2pv2hja-uc.a.run.app/files",
        params={"sender_type": "crawler", "sender_id": "RFB"},
    )
    if res.status_code != 200:
        raise ValueError("Failed to get dates seen")
    already_seen = res.json()
    filtered = set()
    for date in already_seen:
        if date:
            filtered.add(date.split("/")[0])
    return filtered


def has_new_crawl() -> bool:
    return not get_last_modified_date() in get_dates_seen()


def _download(url: str):
    wget.download(url, DOWNLOAD_FOLDER + "/")


def download_all():
    links = get_download_links()

    with Pool() as p:
        p.map(_download, links)
