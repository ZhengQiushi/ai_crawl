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
    print(f"Processed {url}, length: {len(html)}")


async def main():
    crawler = Crawler(
        max_threads=4,
        max_concurrent_per_thread=10,
        max_depth=2,
        timeout=120
    )
    
    start_urls = [
        # "https://www.creatif.com",
        "https://www.fortleenj.org"
    ]
    
    await crawler.crawl_website(start_urls, example_callback)


if __name__ == "__main__":
    asyncio.run(main())