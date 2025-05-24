import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from playwright.async_api import async_playwright
from urllib.parse import urlparse, urljoin
from typing import List, Set, Dict, Optional
import logging
import threading
import time
from bs4 import BeautifulSoup
import asyncio
import logging
import requests
from link_extractor import LinkExtractor
from url_type_checker import *
from crawler import *
from enum import Enum



async def example_callback(url: str, html: str):
    """示例回调函数，简单保存页面"""
    # if "https://www.creatif.com/livingston-nj/camp-calendar" in url:
    #     print(html)
    print(f"Processed {url}, length: {len(html)}")


async def main():
    crawler = Crawler(
        max_processes=1,
        max_concurrent_per_thread=1,
        max_depth=2,
        timeout=10,
        batch_size=5,
        max_retries=3
    )
    
    start_urls = [
        # "https://www.creatif.com/livingston-nj/",
        # "https://www.fortleenj.org",
        "http://www.countrysidechildcarenj.com/",
        # "http://grochowiczfarms.com",
        # "http://baseballrubbingmud.com/",
        # "https://macsdefense.com/",
        # "https://www.wssweddings.com/",
        # "https://www.frogrentscanoeskayaks.com/",
    ]
    
    await crawler.crawl_website(start_urls, example_callback)


if __name__ == "__main__":
    asyncio.run(main())