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

import global_vars
import argparse
import logging, multiprocessing

async def main():
    parser = argparse.ArgumentParser(description='Run the server.')
    parser.add_argument('--County', required=True, type=str, help='Comma-separated list of Counties')
    parser.add_argument('--State', required=True, type=str, help='State')
    parser.add_argument('--Num', required=True, type=str, help='Number of items to retry')
    parser.add_argument('--Config', required=True, type=str, help='Configuration file path')
    args = parser.parse_args()

    global_vars.init_globals(args.Config)

    crawler = Crawler(
        max_processes=1,
        max_concurrent_per_thread=1,
        max_depth=0,
        timeout=10,
        batch_size=5,
        max_retries=3
    )
    
    start_urls = [
        {
            "businessID": 204454,
            "businessFullName": "Fort Lee Borough",
            "domain": "fortleenj.org",
            # "website": "https://www.fortleenj.org/DocumentCenter/View/5453/Business-Registration-081522-PDF"
            "website": "https://www.fortleenj.org/DocumentCenter/View/4156/FEMA-Assistance-for-NJ-Survivors-Affected-by-Hurricane-Ida-PDF"
        }
        # "https://www.creatif.com/livingston-nj/",
        # "https://www.fortleenj.org",
        # "http://www.countrysidechildcarenj.com/",
        # "http://grochowiczfarms.com",
        # "http://baseballrubbingmud.com/",
        # "https://macsdefense.com/",
        # "https://www.wssweddings.com/",
        # "https://www.frogrentscanoeskayaks.com/",
    ]
    
    crawler.crawl_website(start_urls)


if __name__ == "__main__":
    asyncio.run(main())