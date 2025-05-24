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

import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from playwright.async_api import async_playwright
from urllib.parse import urlparse
from typing import List, Set, Dict
import logging
import threading
import time
import queue

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class Crawler:
    def __init__(
        self,
        max_processes: int = 4,
        max_concurrent_per_thread: int = 10,
        max_depth: int = 3,
        timeout: int = 30,
        batch_size: int = 5,
        max_retries: int = 3
    ):
        self.max_processes = max_processes
        self.max_concurrent_per_thread = max_concurrent_per_thread
        self.max_depth = max_depth
        self.timeout = timeout
        self.batch_size = batch_size
        self.max_retries = max_retries
        
        # Shared queue for start URLs
        self.start_urls_queue = queue.Queue()
        self.shutdown_event = threading.Event()
        self.thread_local = threading.local()
        self.link_extractor = LinkExtractor()
        self.url_type_checker = URLTypeChecker()

        self.user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
    async def init_thread_resources(self):
        """Initialize resources for each thread"""
        logger.debug(f"Initializing thread resources in thread: {threading.current_thread().name}")
        if not hasattr(self.thread_local, 'playwright'):
            logger.debug(f"Launching Playwright in thread: {threading.current_thread().name}")
            self.thread_local.playwright = await async_playwright().start()
            self.thread_local.browser = await self.thread_local.playwright.chromium.launch(headless=True)
            self.thread_local.context = await self.thread_local.browser.new_context()
            logger.debug(f"Playwright launched successfully in thread: {threading.current_thread().name}")
        
        if not hasattr(self.thread_local, 'session'):
            logger.debug(f"Creating aiohttp session in thread: {threading.current_thread().name}")
            self.thread_local.session = aiohttp.ClientSession()
            logger.debug(f"aiohttp session created successfully in thread: {threading.current_thread().name}")

    async def close_thread_resources(self):
        """Close thread resources"""
        logger.debug(f"Closing thread resources in thread: {threading.current_thread().name}")
        if hasattr(self.thread_local, 'context'):
            logger.debug(f"Closing Playwright context in thread: {threading.current_thread().name}")
            await self.thread_local.context.close()
        if hasattr(self.thread_local, 'browser'):
            logger.debug(f"Closing Playwright browser in thread: {threading.current_thread().name}")
            await self.thread_local.browser.close()
        if hasattr(self.thread_local, 'playwright'):
            logger.debug(f"Stopping Playwright in thread: {threading.current_thread().name}")
            await self.thread_local.playwright.stop()
        if hasattr(self.thread_local, 'session'):
            logger.debug(f"Closing aiohttp session in thread: {threading.current_thread().name}")
            await self.thread_local.session.close()
        logger.debug(f"Thread resources closed successfully in thread: {threading.current_thread().name}")

    async def fetch_with_playwright(self, url: str) -> str:
        """Fetch page content with Playwright with retry mechanism"""

        load_result = False  # Store load state result for logging as boolean
        networkidle_result = False
        content = ""

        for attempt in range(self.max_retries):
            page = await self.thread_local.context.new_page()  # No user_agent here.

            try:
                logger.debug(f"Attempt {attempt + 1}/{self.max_retries} for {url} in thread: {threading.current_thread().name}")

                # Attempt initial load if not already successful
                if not load_result:
                    try:
                        await page.goto(url, wait_until='load', timeout=self.timeout * 1000)
                        load_result = True
                        content = await page.content()  # Save content on successful load
                        logger.debug(f"Initial 'load' success for {url}")
                    except Exception as e:
                        logger.warning(f"Initial 'load' failed for {url}: {e}")
                        raise e

                # Wait for networkidle
                try:
                    await page.wait_for_load_state('networkidle', timeout=self.timeout * 1000)
                    networkidle_result = True
                    content = await page.content()  # Overwrite with potentially updated content
                    logger.debug(f"Initial 'networkidle' success for {url}")
                except Exception as e:
                    logger.warning(f"'networkidle' wait failed for {url}: {e}")
                    raise e
                
                break
            except Exception as e:
                logger.error(f"Error fetching {url} (attempt {attempt + 1}): {e} in thread: {threading.current_thread().name}")
                if attempt == self.max_retries - 1:  # Last attempt failed
                    break  # Exit the loop; return "" after finally

            finally:
                await page.close()
        
        logger.debug(f"Playwright fetch time for {url} | Load: {load_result}, NetworkIdle: {networkidle_result} in thread: {threading.current_thread().name}")

        return content  # Return empty string after all retries failed

    async def process_website(self, start_url: str, callback: callable):
        """Process a single website with its own queues and stats"""
        website = urlparse(start_url).netloc
        task_queue = queue.Queue()
        visited_urls = set()
        
        crawl_stats = {
            'start_time': time.time(),
            'total_urls': 1,
            'crawled_count': 0,
            'failed_urls': 0,
            'all_urls': {start_url}
        }

        async def crawl_page(url: str, depth: int):
            """Crawl a single page"""
            start_time = time.time()
            if depth > self.max_depth or self.shutdown_event.is_set():
                logger.debug(f"Reached max depth or shutdown signal. Skipping {url} in thread: {threading.current_thread().name}")
                return

            if url in visited_urls:
                logger.debug(f"Already visited {url}. Skipping in thread: {threading.current_thread().name}")
                return
            visited_urls.add(url)

            should_crawl = True
            html = None

            html_type = await self.url_type_checker.is_pdf_url(url)
            if html_type != URLType.HTML:
                logger.info(f"Skipping doc {html_type} URL: {url} in thread: {threading.current_thread().name}")
                should_crawl = False
            else:
                logger.debug(f"Fetching {url} with Playwright in thread: {threading.current_thread().name}")
                html = await self.fetch_with_playwright(url)
                if not html:
                    should_crawl = False
                    crawl_stats['failed_urls'] += 1
                    logger.info(f"Failed parse URL: {url} in thread: {threading.current_thread().name}")

            crawl_stats['crawled_count'] += 1
            end_time = time.time()
            fetch_time = end_time - start_time
            logger.info(f"Crawling {url} at depth {depth} - {website} - Crawled: {crawl_stats['crawled_count']}/{crawl_stats['total_urls']} - Fetch Time: {fetch_time:.2f} seconds in thread: {threading.current_thread().name}")


            if should_crawl and html:
                logger.debug(f"Calling callback for {url} in thread: {threading.current_thread().name}")
                await callback(url, html)

                if depth < self.max_depth:
                    logger.debug(f"Extracting links from {url} in thread: {threading.current_thread().name}")
                    links = await self.link_extractor.extract_links(html, url)
                    new_links = [link for link in links if link not in visited_urls and link not in crawl_stats['all_urls']]
                    
                    crawl_stats['all_urls'].update(new_links)
                    crawl_stats['total_urls'] += len(new_links)

                    logger.debug(f"Adding {len(new_links)} new links to queue for {url} in thread: {threading.current_thread().name}")
                    for i in range(0, len(new_links), self.batch_size):
                        batch = new_links[i:i + self.batch_size]
                        task_queue.put((batch, depth + 1))

                        logger.debug(f"Adding {batch} new links to queue for {url} as batch {i} in thread: {threading.current_thread().name}")
                else:
                    logger.debug(f"Max depth reached, not extracting links from {url} in thread: {threading.current_thread().name}")
            else:
                logger.debug(f"Skipping callback and link extraction for {url} in thread: {threading.current_thread().name}")

        async def thread_worker():
            """Worker that processes URLs from the queue"""
            logger.debug(f"Thread worker started in thread: {threading.current_thread().name}")
            await self.init_thread_resources()
            try:
                while not self.shutdown_event.is_set():
                    try:
                        batch, depth = task_queue.get(timeout=1.0)
                        logger.debug(f"Got batch of {len(batch)} URLs from queue at depth {depth} in thread: {threading.current_thread().name}")
                        tasks = [crawl_page(url, depth) for url in batch]
                        logger.debug(f"Creating {len(tasks)} crawl tasks for batch in thread: {threading.current_thread().name}")
                        await asyncio.gather(*tasks, return_exceptions=True)
                        task_queue.task_done()
                        logger.debug(f"Batch processing completed in thread: {threading.current_thread().name}")
                    except queue.Empty:
                        # Check if we should exit
                        if crawl_stats['crawled_count'] >= crawl_stats['total_urls']:
                            logger.debug(f"Crawled all URLs, exiting thread worker in thread: {threading.current_thread().name}")
                            break
                        logger.debug(f"Queue is empty, continuing in thread: {threading.current_thread().name} {crawl_stats['crawled_count']} / {crawl_stats['total_urls']}")
                        continue
            except Exception as e:
                logger.error(f"Thread worker error for {website}: {e} in thread: {threading.current_thread().name}")
            finally:
                await self.close_thread_resources()
                logger.debug(f"Thread worker finished in thread: {threading.current_thread().name}")

        # Start with initial URL
        logger.info(f"Starting crawl for {start_url} in thread: {threading.current_thread().name}")
        task_queue.put(([start_url], 0))

        # Create thread pool for this website
        with ThreadPoolExecutor(max_workers=self.max_concurrent_per_thread) as executor:
            loop = asyncio.get_event_loop()
            tasks = []
            
            for _ in range(self.max_concurrent_per_thread):
                task = loop.run_in_executor(
                    executor,
                    lambda: asyncio.new_event_loop().run_until_complete(thread_worker())
                )
                tasks.append(task)
            
            try:
                logger.info(f"Waiting for thread workers to complete for {website} in thread: {threading.current_thread().name}")
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=self.timeout * 100)
            except asyncio.TimeoutError:
                logger.warning(f"Timeout reached for website {website} in thread: {threading.current_thread().name}")
                self.shutdown_event.set()
            
            finally:
                logger.info(f"Cancelling remaining tasks for {website} in thread: {threading.current_thread().name}")
                for task in tasks:
                    if not task.done():
                        task.cancel()

        # Print stats for this website
        end_time = time.time()
        total_time = end_time - crawl_stats['start_time']
        average_time = total_time / crawl_stats['crawled_count'] if crawl_stats['crawled_count'] else 0

        logger.info(f"\nCrawling statistics for {website}:")
        logger.info(f"  Total time: {total_time:.2f} seconds")
        logger.info(f"  Total URLs crawled: {crawl_stats['crawled_count']}")
        logger.info(f"  Failed URLs: {crawl_stats['failed_urls']}")
        logger.info(f"  Total URLs found: {crawl_stats['total_urls']}")
        logger.info(f"  Average time per page: {average_time:.2f} seconds")

    async def website_worker(self, callback: callable):
        """Worker that processes websites from the start_urls_queue"""
        logger.info(f"Website worker started in process: {threading.current_thread().name}")
        while not self.shutdown_event.is_set():
            try:
                start_url = self.start_urls_queue.get_nowait()
                logger.info(f"Got website {start_url} from queue in process: {threading.current_thread().name}")
                await self.process_website(start_url, callback)
                self.start_urls_queue.task_done()
                logger.info(f"Finished processing website {start_url} in process: {threading.current_thread().name}")
            except queue.Empty:
                logger.info(f"Start URL queue is empty, exiting website worker in process: {threading.current_thread().name}")
                break
            except Exception as e:
                logger.error(f"Error in website worker: {e} in process: {threading.current_thread().name}")

    async def crawl_website(self, start_urls: List[str], callback: callable):
        """Main crawl method"""
        logger.info("Starting crawl process")
        self.shutdown_event.clear()
        
        # Add all start URLs to the queue
        for url in start_urls:
            self.start_urls_queue.put(url)
            logger.info(f"Added {url} to start URL queue")

        # Create process pool
        with ThreadPoolExecutor(max_workers=self.max_processes) as executor:
            loop = asyncio.get_event_loop()
            tasks = []
            
            # Each process will work on websites until the queue is empty
            for _ in range(self.max_processes):
                task = loop.run_in_executor(
                    executor,
                    lambda: asyncio.new_event_loop().run_until_complete(self.website_worker(callback))
                )
                tasks.append(task)
            
            try:
                logger.info(f"Waiting for website workers to complete")
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=self.timeout * 20)
            except asyncio.TimeoutError:
                logger.warning("Overall crawl timeout reached")
                self.shutdown_event.set()
            finally:
                logger.info("Cancelling remaining website worker tasks")
                for task in tasks:
                    if not task.done():
                        task.cancel()
        logger.info("Crawl process finished")