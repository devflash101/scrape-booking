import subprocess
import sys

command = "python3 -m pip install curl-cffi lxml -U --pre --user"
command = f"{command} --break-system-packages" if sys.version_info >= (3, 10) else command  
subprocess.run(command, shell=True, check=True)
###################################################################################

import asyncio
import json
import logging
from dataclasses import dataclass
from itertools import chain, count
from time import time

from curl_cffi import requests
from lxml import html


PROXY = {
    "http": "socks5://modernstyle:dk8864taxik3b_region-europe_streaming-1@geo.iproyal.com:12325",
    "https": "socks5://modernstyle:dk8864taxik3b_region-europe_streaming-1@geo.iproyal.com:12325",
    "no_proxy": "localhost,127.0.0.1",
}
#PROXY = None
REQUESTS_COUNTER = count(1)


@dataclass
class Hotel:
    url: str = ""
    image: str = ""
    name: str = ""
    address: str = ""
    stars: str = ""
    price: str = ""
    reviews_count: str = ""
    reviews_grade: str = ""
    reviews_specific_grades: str = ""
    sustainable_level: str = ""
    most_popular_facilities: str = ""
    highlights: str = ""
    

def get_url(url):
    for _ in range(2):
        try:
            with requests.Session(impersonate="chrome", proxies=PROXY) as s:
                resp = s.get(url)
                logging.debug(f"get_url() {resp.status_code} {len(resp.content)} {resp.elapsed} \t{next(REQUESTS_COUNTER)}")
                resp.raise_for_status()
                return resp.content
        except Exception as ex:
            logging.warning(f"get_url() {type(ex).__name__}: {ex}")


async def aget_url(url, s):
    for _ in range(5):
        try:
            resp = await s.get(url, stream=True)
            resp.raise_for_status()
            page_content = await resp.acontent()
            logging.debug(f"aget_url() {resp.status_code} {len(page_content)} {resp.elapsed} \t{next(REQUESTS_COUNTER)}")
            return page_content
        except Exception as ex:
            logging.warning(f"aget_url() {type(ex).__name__}: {ex}")


def parse_hotels_count(url):
    """get hotels count from search page"""
    page_content = get_url(url)
    tree = html.fromstring(page_content)
    # get number of founded objects
    h1_text = x[0] if (x := tree.xpath("//h1//text()")) else ""
    hotels_count = int("".join(x for x in h1_text if x.isdigit()))
    if hotels_count < 100:
        # get number of pages in pagination and multiply by 25
        pages = tree.xpath("//div[@data-testid='pagination']//li[last()]//text()")
        hotels_count = int(pages[0].strip()) * 25
    return hotels_count


async def aparse_hotel_urls_page(url, s):
    """async get hotel urls from search page"""
    page_content = await aget_url(url, s)
    tree = html.fromstring(page_content)
    hotel_urls = tree.xpath("//div[contains(@data-testid, 'property-card-container')]//h3/a/@href")
    return hotel_urls


async def aparse_hotel(url, s):
    """Parse hotel data from hotel url. Returns Hotel dataclass object"""
    url = f"{url}&selected_currency=USD" if "selected_currency" not in url else url
    h = Hotel()
    for _ in range(2):
        try:
            page_content = await aget_url(url, s)
            tree = html.fromstring(page_content)
            ht = tree.xpath("//div[@class='hotelchars']")[0]
            h.url = url
            image = ht.xpath(".//a[@data-preview-image-ranking='1']/img/@src")
            h.image = image[0] if image else ""
            name = ht.xpath(".//div[contains(@id, 'hotel_name')]//h2//text()")
            h.name = name[0] if name else ""
            address = ht.xpath(".//span[contains(@class, 'address')]//text()")
            h.address = address[0].strip() if address else ""
            stars = ht.xpath(".//span[contains(@data-testid, 'rating')]//span")
            h.stars = f"{len(stars)}" if stars else ""
            price = ht.xpath(".//tbody//tr[1]//div[contains(@class, 'price')]//span//text()")
            h.price = "".join(x for x in price[0] if x.isdigit()) if price else ""
            reviews_count = ht.xpath(".//div[contains(@data-testid, 'review-score')]//span[last()]//text()")
            h.reviews_count = "".join(x for x in reviews_count[-1] if x.isdigit()) if reviews_count else ""
            reviews_grade = ht.xpath(".//div[contains(@data-testid, 'review-score')]/div[1]//text()")
            h.reviews_grade = reviews_grade[0] if reviews_grade else ""
            reviews_specific_grades = ht.xpath(".//div[contains(@data-testid, 'PropertyReviewsRegionBlock')]//div[@data-testid='review-subscore']//text()")
            h.reviews_specific_grades = ", ".join(reviews_specific_grades).replace(",  ,", ":") if reviews_specific_grades else ""
            sustainable_level = ht.xpath(".//span[contains(text(), 'Sustainable')]//text()")
            h.sustainable_level = "".join(x for x in sustainable_level[0] if x.isdigit() or x == "+") if sustainable_level else ""
            most_popular_facilities = ht.xpath(".//section[contains(@id, 'facilities')]//div[contains(@data-testid, 'facilities')]//span//text()")
            h.most_popular_facilities = ", ".join(sorted(x for e in most_popular_facilities if (x := e.strip()))) if most_popular_facilities else ""
            highlights = ht.xpath(".//div[contains(@class, 'property-highlights')]//span[contains(@class, 'item')]//text()")
            h.highlights = ", ".join(x for e in highlights if (x := e.strip())) if highlights else ""
            break
        except Exception as ex:
            url = url.split("&")[0]
            h.url = url
            h.image = "Error"
            print(f"aparse_hotel() {url} {type(ex).__name__}: {ex}")
    return json.dumps(h.__dict__)


async def aparse_hotels(url):
    hotels_count = parse_hotels_count(url)
    async with requests.AsyncSession(impersonate="chrome", proxies=PROXY) as s:
        tasks = []
        for offset in range(0, min(hotels_count, LIMIT), 25):
            task = asyncio.create_task(aparse_hotel_urls_page(f"{url}&offset={offset}", s))
            tasks.append(task)      
        hotel_urls = await asyncio.gather(*tasks)

        tasks_hotels = []
        for hotel_url in chain.from_iterable(hotel_urls):
            task_hotel = asyncio.create_task(aparse_hotel(hotel_url, s))
            tasks_hotels.append(task_hotel)
        hotels = await asyncio.gather(*tasks_hotels)
    return hotels


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    t0 = time()
    with open("input.json") as input_file:
        input_data = json.load(input_file)
        urls = input_data["input"]
        try:
            LIMIT = int(input_data["options"].get("limit", 10000))
        except:
            LIMIT = 10000

    results = []
    for url in urls:
        temp_results = asyncio.run(aparse_hotels(url))
        results.extend(temp_results)

    with open("output.json", "w") as output_file:
        output_file.write("[\n")
        for i, h in enumerate(results):
            if i > 0:
                output_file.write(",\n")
            json.dump(json.loads(h), output_file, indent=2, ensure_ascii=False)
        output_file.write("\n]")
    print(f"Done {len(results)} results in {time() - t0} sec.")
