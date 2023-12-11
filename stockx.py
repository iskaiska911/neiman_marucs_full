import json
import math
import time
from typing import Dict, List
from urllib.parse import urlparse,urljoin,urlunparse, parse_qs, urlencode
from bs4 import BeautifulSoup
import asyncio





import requests
from loguru import logger as log
from nested_lookup import nested_lookup
from scrapfly import ScrapeApiResponse, ScrapeConfig, ScrapflyClient

from tools import get_category_paths
from decouple import config

SCRAPFLY = ScrapflyClient(key=config('SCRAPFLY_KEY'))
BASE_CONFIG = {
    "asp": True,
}
selector = 'a.product-thumbnail__link'
selector_button = 'nav.pagination>span>span'
selector_next_button = 'a[class="arrow-button--right"]'

def parse_nextjs(result: ScrapeApiResponse) -> Dict:
    time.sleep(1)
    data = result.selector.css("script#__NEXT_DATA__::text").get()
    if not data:
        data = result.selector.css("script[data-name=query]::text").get()
        data = data.split("=", 1)[-1].strip().strip(";")
    data = json.loads(data)
    return data


def parse_urls(result: ScrapeApiResponse, selector) -> List:
    models = []
    try:
        for element in result.soup.select(selector):
            parsed_url = urlparse(element.get('href')[1:])
            slug = parsed_url.path[1:][parsed_url.path[1:].index('/'):]

            models.append(
                {
                    "slug": slug,
                    "parsed": False
                }

            )
    except Exception as e:
        log.info(e)
    return models


def max_page(result: ScrapeApiResponse, selector_button):
    try:
        max_page = result.soup.select('nav.pagination>span')[1].text.replace('1 of ','')
        return int(max_page)
    except Exception as e:
        log.info(e)
        return 1


def get_all_categories():
    response = SCRAPFLY.scrape(ScrapeConfig(url='https://www.neimanmarcus.com/en-kz',country='GB',asp=True ))
    assert response.status_code == 200
    categories = response.soup.select('a.silo-link')
    categories = {i.text.lower():i.attrs['href'] for i in categories}
    return categories

def get_all_subcategories(catgegory):
    response = SCRAPFLY.scrape(ScrapeConfig(url=f'https://www.neimanmarcus.com/en-kz/c{catgegory}',country='GB',asp=True))
    assert response.status_code == 200
    subcategories = response.soup.select('ul.left-nav__category>li>ul>li>a')
    subcategories = {i.text:i.attrs['href'] for i in subcategories}
    return subcategories

def get_all_thirdlevel(catgegory):
    response = SCRAPFLY.scrape(ScrapeConfig(url=f'https://www.neimanmarcus.com/en-kz/c{catgegory}',country='GB',asp=True))
    assert response.status_code == 200
    thirdlevel = response.soup.select('ul.left-nav__category>li>ul>li>a')
    thirdlevel = {i.text:i.attrs['href'] for i in thirdlevel}
    return thirdlevel

async def scrape_slugs(url: str) -> list:
    slugs = list()
    log.info("scraping slug {}", url)
    res = await SCRAPFLY.async_scrape(ScrapeConfig(url, **BASE_CONFIG))
    page = max_page(res, selector_button)
    for i in range(1, page + 1):
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        query_params['page'] = [str(i)]
        new_query_string = urlencode(query_params, doseq=True)
        modified_url = urlunparse((parsed_url.scheme,parsed_url.netloc,parsed_url.path,parsed_url.params,new_query_string,parsed_url.fragment))

        result = await SCRAPFLY.async_scrape(ScrapeConfig(modified_url, **BASE_CONFIG,retry=True,country='GB'))
        log.info("Requesting {} Status {}".format(url + f"?page={i}&source=leftNav", result.status_code))
        slugs.extend(parse_urls(result, selector))
    return slugs


async def scrape_product(url: str) -> Dict:
    log.info(f"Scraping product  {url}")
    time.sleep(1)
    try:
        result = await SCRAPFLY.async_scrape(ScrapeConfig(url, **BASE_CONFIG,render_js=True,rendering_wait=5000,retry=True,country='GB'))
        data = result.soup.select('''[id='state']''')[0].text
        if not data:
            log.warning(f"Empty response content for {url}")
            return get_default_product(url, "Empty response content")

        structure = BeautifulSoup(data, 'html.parser')
        #script_tag = structure.find('script', {'id': 'state'})
        #json_content = script_tag.string
        data_json = json.loads(structure.text)


        log.info("Requesting {} Status {}".format(url,result.status_code))
        try:
            product = {"urs": url,
               "name": data_json['productCatalog']['product']['name'],
               "slug": data_json['productCatalog']['product']['id'],
               "price": data_json['productCatalog']['product']['price'],
               "productTypeId":data_json['productCatalog']['product']['metadata']['masterStyle']
                   }
        except:
            product = {"urs": url,
                   "name":"",
                   "slug": "",
                   "price": "",
                   "productTypeId": ""
                   }

        product["gender"] =  data_json['srp']['search']['gender']
        try:
            product["brand"] = data_json['productCatalog']['product']['linkedData']['brand'],
        except:
            product["brand"] = ""
        try:
            product["description"] = data_json['productCatalog']['product']['linkedData']['description']
        except:
            product["description"] = ""
        try:
            product["category"] = data_json['productCatalog']['product']['hierarchy'][0]
        except:
            product["category"] = ""
        try:
            product["characteristics"] = data_json['productCatalog']['product']['details']['longDesc']
        except:
            product["characteristics"] = ""
        try:
            product["images"] =[data_json['productCatalog']['product']['options']['productOptions'][1]['values'][0]['media']['main']['dynamic']['url'],
            data_json['productCatalog']['product']['options']['productOptions'][1]['values'][0]['media']['main']['thumbnail']['url'],
            data_json['productCatalog']['product']['options']['productOptions'][1]['values'][0]['media']['main']['medium']['url'],
            data_json['productCatalog']['product']['options']['productOptions'][1]['values'][0]['media']['main']['large']['url']]
        except:
            product["images"] = ""
        try:
            product['variants'] = [i['name'] for i in data_json['productCatalog']['product']['options']['productOptions'][0]['values']]
        except:
            product['variants'] = ""
        try:
            product['quantity'] = data_json['productCatalog']['product']['quantity']
        except:
            product['quantity'] = ""

    except json.decoder.JSONDecodeError as json_error:
        product = {
            "urs": url,
            "name": "",
            "slug": "",
            "price": "",
            "productTypeId": "",
            "gender": "",
            "brand": "",
            "description": "",
            "category": "",
            "characteristics": "",
            "images": "",
            "variants": "",
            "quantity": ""
        }
        return product

    except Exception as e:
            log.error("Error scraping %s: %s", url, e)

            product = {
        "urs": url,
        "name": "",
        "slug": "",
        "price": "",
        "productTypeId": "",
        "gender": "",
        "brand": "",
        "description": "",
        "category": "",
        "characteristics": "",
        "images": "",
        "variants": "",
        "quantity": ""
    }
            return product

    return product




async def scrape_search(url: str, max_pages: int = 25) -> List[Dict]:
    log.info("scraping search {}", url)
    first_page = await SCRAPFLY.async_scrape(ScrapeConfig(url, **BASE_CONFIG))
    data = parse_nextjs(first_page)
    _first_page_results = nested_lookup("results", data)[0]
    _paging_info = _first_page_results["pageInfo"]
    total_pages = _paging_info["pageCount"] or math.ceil(_paging_info["total"] / _paging_info["limit"])
    if max_pages < total_pages:
        total_pages = max_pages

    product_previews = [edge["node"] for edge in _first_page_results["edges"]]

    log.info("scraping search {} pagination ({} more pages)", url, total_pages - 1)
    _other_pages = [
        ScrapeConfig(f"{first_page.context['url']}&page={page}", **BASE_CONFIG)
        for page in range(2, total_pages + 1)
    ]
    async for result in SCRAPFLY.concurrent_scrape(_other_pages):
        data = parse_nextjs(result)
        _page_results = nested_lookup("results", data)[0]
        product_previews.extend([edge["node"] for edge in _page_results["edges"]])
    log.info("scraped {} products from {}", len(product_previews), url)
    return product_previews


def get_default_product(url: str, reason: str) -> Dict:
    log.warning(f"Returning default product for %s due to:{url},{reason}")
    return {
        "urs": url,
        "name": "",
        "slug": "",
        "price": "",
        "productTypeId": "",
        "gender": "",
        "brand": "",
        "description": "",
        "category": "",
        "characteristics": "",
        "images": "",
        "variants": "",
        "quantity": ""
    }