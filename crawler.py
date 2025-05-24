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
import queue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Crawler:
    """
    Crawls web pages using Playwright and aiohttp.
    """

    def __init__(
        self,
        max_processes: int = 4,
        max_concurrent_per_thread: int = 10,
        max_depth: int = 3,
        timeout: int = 30,
        batch_size: int = 5 # Added batch size
    ):
        self.max_processes = max_processes
        self.max_concurrent_per_thread = max_concurrent_per_thread
        self.max_depth = max_depth
        self.timeout = timeout
        self.batch_size = batch_size
        self.visited_urls: Set[str] = set()
        self.thread_local = threading.local()
        self.crawl_stats: Dict[str, Dict] = {}  # 存储每个网站的爬取统计信息
        self.lock = threading.Lock() # add lock
        self.link_extractor = LinkExtractor()
        self.url_type_checker = URLTypeChecker()
        self.task_queue = queue.Queue() # queue for urls

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

                # Enqueue the new links in batches
                for i in range(0, len(new_links), self.batch_size):
                    batch = new_links[i:i + self.batch_size]
                    self.task_queue.put((batch, depth + 1, callback, website))

    async def _thread_worker(self, website: str):
        """线程工作函数 - 从队列中获取URL进行处理"""
        await self.init_thread_resources()
        try:
            while True:
                batch, depth, callback, website = self.task_queue.get()
                try:
                    tasks = [self.crawl_page(url, depth, callback, website) for url in batch]
                    await asyncio.gather(*tasks, return_exceptions=True)  # 并发处理一批URL
                except Exception as e:
                    logger.error(f"Error processing batch of URLs: {e}")
                finally:
                    self.task_queue.task_done()
        except Exception as e:
            logger.error(f"Thread worker error: {e}")
        finally:
            await self.close_thread_resources()

    async def website_process(self, start_url: str, callback: callable):
        """
        处理单个网站的主流程

        :param start_url: 网站的起始URL
        :param callback: 处理每个页面的回调函数
        """
        website = urlparse(start_url).netloc

        # 初始化统计数据
        with self.lock:
            self.crawl_stats[website] = {
                'start_time': time.time(),
                'total_urls': 1,  # 初始URL算一个
                'crawled_count': 0,
                'all_urls': {start_url},
                'total_fetch_time': 0.0
            }
        
        # 将起始URL放入任务队列
        self.task_queue.put(([start_url], 0, callback, website))

        # 创建线程池，用于处理URL
        with ThreadPoolExecutor(max_workers=self.max_concurrent_per_thread) as executor:
            loop = asyncio.get_event_loop()
            tasks = [loop.run_in_executor(executor, lambda: asyncio.new_event_loop().run_until_complete(self._thread_worker(website))) for _ in range(self.max_concurrent_per_thread)]

            # 等待所有任务完成
            await asyncio.gather(*tasks)
        
        # 等待队列所有任务完成
        self.task_queue.join()


        # 输出统计信息
        end_time = time.time()
        total_time = end_time - self.crawl_stats[website]['start_time']
        average_time = total_time / self.crawl_stats[website]['crawled_count'] if self.crawl_stats[website]['crawled_count'] else 0

        logger.info(f"Crawling statistics for {website}:")
        logger.info(f"  Total time: {total_time:.2f} seconds")
        logger.info(f"  Total URLs crawled: {self.crawl_stats[website]['crawled_count']}")
        logger.info(f"  Total URLs found: {self.crawl_stats[website]['total_urls']}")
        logger.info(f"  Average time per page: {average_time:.2f} seconds")


    async def crawl_website(self, start_urls: List[str], callback: callable):
        """
        主爬取方法：为每个网站分配一个进程

        :param start_urls: 起始URL列表
        :param callback: 处理每个页面的回调函数，接收(url, html)参数
        """
        # 创建网站进程池
        with ThreadPoolExecutor(max_workers=self.max_processes) as executor:
            loop = asyncio.get_event_loop()
            tasks = []

            # 为每个网站创建一个进程任务
            for url in start_urls:
                task = loop.run_in_executor(
                    executor,
                    lambda u=url: asyncio.new_event_loop().run_until_complete(
                        self.website_process(u, callback)
                    )
                )
                tasks.append(task)
            
            # 等待所有任务完成
            await asyncio.gather(*tasks)