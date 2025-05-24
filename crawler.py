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
from enum import Enum


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Crawler:
    """
    Crawls web pages using Playwright and aiohttp.
    """

    def __init__(
        self,
        max_threads: int = 4,
        max_concurrent_per_thread: int = 10,
        max_depth: int = 3,
        timeout: int = 30,
    ):
        self.max_threads = max_threads
        self.max_concurrent_per_thread = max_concurrent_per_thread
        self.max_depth = max_depth
        self.timeout = timeout
        self.visited_urls: Set[str] = set()
        self.thread_local = threading.local()
        self.crawl_stats: Dict[str, Dict] = {}  # 存储每个网站的爬取统计信息
        self.lock = threading.Lock() # add lock
        self.link_extractor = LinkExtractor()
        self.url_type_checker = URLTypeChecker()

    async def init_thread_resources(self):
        """为每个线程初始化资源"""
        if not hasattr(self.thread_local, 'playwright'):
            self.thread_local.playwright = await async_playwright().start()
            self.thread_local.browser = await self.thread_local.playwright.chromium.launch(headless=True)
            self.thread_local.context = await self.thread_local.browser.new_context()
        
        if not hasattr(self.thread_local, 'session'):
            self.thread_local.session = aiohttp.ClientSession()

    async def close_thread_resources(self):
        """关闭线程的资源"""
        if hasattr(self.thread_local, 'context'):
            await self.thread_local.context.close()
        if hasattr(self.thread_local, 'browser'):
            await self.thread_local.browser.close()
        if hasattr(self.thread_local, 'playwright'):
            await self.thread_local.playwright.stop()
        if hasattr(self.thread_local, 'session'):
            await self.thread_local.session.close()

    async def fetch_with_playwright(self, url: str) -> str:
        """使用Playwright获取页面内容，并记录时间"""
        start_time = time.time()
        page = await self.thread_local.context.new_page()
        try:
            await page.goto(url, wait_until='networkidle', timeout=self.timeout * 1000)
            content = await page.content()
            end_time = time.time()
            fetch_time = end_time - start_time
            logger.info(f"Playwright fetch time for {url}: {fetch_time:.2f} seconds")
            return content
        except Exception as e:
            logger.error(f"Error fetching {url} with Playwright: {e}")
            return ""
        finally:
            await page.close()

    async def crawl_page(self, url: str, depth: int, callback: callable, website: str):
        """爬取单个页面"""
        if depth > self.max_depth:
            return

        with self.lock:
            if url in self.visited_urls:
                return

            self.visited_urls.add(url)

        should_crawl = True
        html = None  # Initialize html to None

        html_type = await self.url_type_checker.is_pdf_url(url)
        if html_type != URLType.HTML:
            logger.info(f"Skipping doc {html_type} URL: {url}")
            should_crawl = False  # Set flag to indicate skipping the crawling
        else:
            html = await self.fetch_with_playwright(url)
            if not html:
                should_crawl = False  # Do not proceed with crawling

        with self.lock:
            self.crawl_stats[website]['crawled_count'] += 1
            total_to_crawl = self.crawl_stats[website]['total_urls']
            crawled_count = self.crawl_stats[website]['crawled_count']

        logger.info(f"Crawling {url} at depth {depth} - {website} - Crawled: {crawled_count}/{total_to_crawl}")

        if should_crawl and html:  # Only proceed if NOT a PDF and HTML exists
            await callback(url, html)

            if depth < self.max_depth:
                links = await self.link_extractor.extract_links(html, url)
                new_links = []
                with self.lock:
                    for link in links:
                        if link not in self.visited_urls and link not in self.crawl_stats[website]['all_urls']:
                            new_links.append(link)
                            self.crawl_stats[website]['all_urls'].add(link)
                with self.lock:
                    self.crawl_stats[website]['total_urls'] += len(new_links)

                tasks = [self.crawl_page(link, depth + 1, callback, website) for link in new_links]
                await asyncio.gather(*tasks, return_exceptions=True)  # 添加return_exceptions=True

    async def _thread_worker(self, url: str, callback: callable, website: str):
        """单个线程的工作函数 - 只处理单个URL"""
        await self.init_thread_resources()
        try:
            await self.crawl_page(url, 0, callback, website)
        except Exception as e:
            logger.error(f"Error processing {url}: {e}")
        finally:
            await self.close_thread_resources()

    async def crawl_website(self, start_urls: List[str], callback: callable):
        """
        主爬取方法
        
        :param start_urls: 起始URL列表
        :param callback: 处理每个页面的回调函数，接收(url, html)参数
        """
        # 初始化统计数据
        for url in start_urls:
            website = urlparse(url).netloc
            with self.lock:
                self.crawl_stats[website] = {
                    'start_time': time.time(),
                    'total_urls': 1,  # 初始URL算一个
                    'crawled_count': 0,
                    'all_urls': set(start_urls),
                    'total_fetch_time': 0.0
                }
        
        # 创建线程池
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            loop = asyncio.get_event_loop()
            tasks = []
            
            # 为每个URL创建一个线程任务
            for url in start_urls:
                website = urlparse(url).netloc # get netloc
                task = loop.run_in_executor(
                    executor,
                    lambda u=url, w=website: asyncio.new_event_loop().run_until_complete(
                        self._thread_worker(u, callback, w)
                    )
                )
                tasks.append(task)
            
            # 等待所有任务完成
            await asyncio.gather(*tasks)

        # 输出统计信息
        for website, stats in self.crawl_stats.items():
            end_time = time.time()
            total_time = end_time - stats['start_time']
            average_time = total_time / stats['crawled_count'] if stats['crawled_count'] else 0
            
            logger.info(f"Crawling statistics for {website}:")
            logger.info(f"  Total time: {total_time:.2f} seconds")
            logger.info(f"  Total URLs crawled: {stats['crawled_count']}")
            logger.info(f"  Total URLs found: {stats['total_urls']}") # updated urls
            logger.info(f"  Average time per page: {average_time:.2f} seconds")
