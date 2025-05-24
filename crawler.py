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
    def __init__(
        self,
        max_processes: int = 4,
        max_concurrent_per_thread: int = 10,
        max_depth: int = 3,
        timeout: int = 30,
        batch_size: int = 5
    ):
        self.max_processes = max_processes
        self.max_concurrent_per_thread = max_concurrent_per_thread
        self.max_depth = max_depth
        self.timeout = timeout
        self.batch_size = batch_size
        self.visited_urls: Set[str] = set()
        self.thread_local = threading.local()
        self.crawl_stats: Dict[str, Dict] = {}
        self.lock = threading.Lock()
        self.link_extractor = LinkExtractor()
        self.url_type_checker = URLTypeChecker()
        self.task_queue = queue.Queue()
        self.shutdown_event = threading.Event()  # Add shutdown event

    async def init_thread_resources(self):
        """Initialize resources for each thread"""
        if not hasattr(self.thread_local, 'playwright'):
            self.thread_local.playwright = await async_playwright().start()
            self.thread_local.browser = await self.thread_local.playwright.chromium.launch(headless=True)
            self.thread_local.context = await self.thread_local.browser.new_context()
        
        if not hasattr(self.thread_local, 'session'):
            self.thread_local.session = aiohttp.ClientSession()

    async def close_thread_resources(self):
        """Close thread resources"""
        if hasattr(self.thread_local, 'context'):
            await self.thread_local.context.close()
        if hasattr(self.thread_local, 'browser'):
            await self.thread_local.browser.close()
        if hasattr(self.thread_local, 'playwright'):
            await self.thread_local.playwright.stop()
        if hasattr(self.thread_local, 'session'):
            await self.thread_local.session.close()

    async def fetch_with_playwright(self, url: str) -> str:
        """Fetch page content with Playwright"""
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
        """Crawl a single page"""
        if depth > self.max_depth or self.shutdown_event.is_set():
            return

        with self.lock:
            if url in self.visited_urls:
                return
            self.visited_urls.add(url)

        should_crawl = True
        html = None

        html_type = await self.url_type_checker.is_pdf_url(url)
        if html_type != URLType.HTML:
            logger.info(f"Skipping doc {html_type} URL: {url}")
            should_crawl = False
        else:
            html = await self.fetch_with_playwright(url)
            if not html:
                should_crawl = False

        with self.lock:
            self.crawl_stats[website]['crawled_count'] += 1
            total_to_crawl = self.crawl_stats[website]['total_urls']
            crawled_count = self.crawl_stats[website]['crawled_count']

        logger.info(f"Crawling {url} at depth {depth} - {website} - Crawled: {crawled_count}/{total_to_crawl}")

        if should_crawl and html:
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

                for i in range(0, len(new_links), self.batch_size):
                    batch = new_links[i:i + self.batch_size]
                    self.task_queue.put((batch, depth + 1, callback, website))

    async def _thread_worker(self, website: str):
        """Thread worker function"""
        await self.init_thread_resources()
        try:
            while not self.shutdown_event.is_set():  # Check shutdown flag
                try:
                    # Add timeout to queue.get to prevent deadlock
                    batch, depth, callback, website = self.task_queue.get(timeout=1.0)
                    try:
                        tasks = [self.crawl_page(url, depth, callback, website) for url in batch]
                        await asyncio.gather(*tasks, return_exceptions=True)
                    except Exception as e:
                        logger.error(f"Error processing batch of URLs: {e}")
                    finally:
                        self.task_queue.task_done()
                except queue.Empty:
                    # Check if we should exit
                    with self.lock:
                        stats = self.crawl_stats.get(website, {})
                        if stats.get('crawled_count', 0) >= stats.get('total_urls', 1):
                            break
                    continue
        except Exception as e:
            logger.error(f"Thread worker error: {e}")
        finally:
            await self.close_thread_resources()

    async def website_process(self, start_url: str, callback: callable):
        """Process a single website"""
        website = urlparse(start_url).netloc

        with self.lock:
            self.crawl_stats[website] = {
                'start_time': time.time(),
                'total_urls': 1,
                'crawled_count': 0,
                'all_urls': {start_url},
                'total_fetch_time': 0.0
            }
        
        self.task_queue.put(([start_url], 0, callback, website))

        with ThreadPoolExecutor(max_workers=self.max_concurrent_per_thread) as executor:
            loop = asyncio.get_event_loop()
            tasks = []
            
            # Create worker tasks
            for _ in range(self.max_concurrent_per_thread):
                task = loop.run_in_executor(
                    executor,
                    lambda: asyncio.new_event_loop().run_until_complete(self._thread_worker(website))
                )
                tasks.append(task)
            
            # Wait for all tasks to complete or timeout
            try:
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=self.timeout * 10)
            except asyncio.TimeoutError:
                logger.warning(f"Timeout reached for website {website}")
                self.shutdown_event.set()  # Signal shutdown
            
            # Clean up any remaining tasks
            for task in tasks:
                if not task.done():
                    task.cancel()
        
        # Final statistics
        end_time = time.time()
        total_time = end_time - self.crawl_stats[website]['start_time']
        crawled_count = self.crawl_stats[website]['crawled_count']
        average_time = total_time / crawled_count if crawled_count else 0

        logger.info(f"\nCrawling statistics for {website}:")
        logger.info(f"  Total time: {total_time:.2f} seconds")
        logger.info(f"  Total URLs crawled: {crawled_count}")
        logger.info(f"  Total URLs found: {self.crawl_stats[website]['total_urls']}")
        logger.info(f"  Average time per page: {average_time:.2f} seconds")

    async def crawl_website(self, start_urls: List[str], callback: callable):
        """Main crawl method"""
        self.shutdown_event.clear()  # Reset shutdown flag
        
        with ThreadPoolExecutor(max_workers=self.max_processes) as executor:
            loop = asyncio.get_event_loop()
            tasks = []

            for url in start_urls:
                task = loop.run_in_executor(
                    executor,
                    lambda u=url: asyncio.new_event_loop().run_until_complete(
                        self.website_process(u, callback)
                    )
                )
                tasks.append(task)
            
            try:
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=self.timeout * 20)
            except asyncio.TimeoutError:
                logger.warning("Overall crawl timeout reached")
                self.shutdown_event.set()
            finally:
                # Ensure all tasks are cancelled if they're still running
                for task in tasks:
                    if not task.done():
                        task.cancel()