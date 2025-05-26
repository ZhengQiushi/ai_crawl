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
import multiprocessing
import os

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
import global_vars
from pipeline import CsvPipeline


class Crawler:
    def __init__(
        self,
        max_processes: int = 4,
        max_concurrent_per_thread: int = 10,
        max_depth: int = 3,
        timeout: int = 30,
        batch_size: int = 5,
        max_retries: int = 3,
        max_pages_per_website: int = 500  # New parameter
    ):
        self.max_processes = max_processes
        self.max_concurrent_per_thread = max_concurrent_per_thread
        self.max_depth = max_depth
        self.timeout = timeout
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.max_pages_per_website = max_pages_per_website  # Set the limit

        # Shared queue for start URLs
        self.start_providers_queue = multiprocessing.Queue()  # Changed to multiprocessing.Queue
        self.shutdown_event = multiprocessing.Event()  # Changed to multiprocessing.Event
        # self.thread_local = threading.local()  # Removing thread local storage to avoid pickling issues
        self.link_extractor = LinkExtractor()
        self.url_type_checker = URLTypeChecker()

        self.user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
    #  Make the following functions static
    @staticmethod
    async def init_process_resources():
        """Initialize resources for each process"""
        process_local = threading.local()

        global_vars.logger.debug(f"Initializing process resources in process: proc-{os.getpid()}-{threading.current_thread().name}")
        if not hasattr(process_local, 'playwright'):
            global_vars.logger.debug(f"Launching Playwright in process: proc-{os.getpid()}-{threading.current_thread().name}")
            process_local.playwright = await async_playwright().start()
            process_local.browser = await process_local.playwright.chromium.launch(headless=True)
            process_local.context = await process_local.browser.new_context()
            global_vars.logger.debug(f"Playwright launched successfully in process: proc-{os.getpid()}-{threading.current_thread().name}")
        
        if not hasattr(process_local, 'session'):
            global_vars.logger.debug(f"Creating aiohttp session in process: proc-{os.getpid()}-{threading.current_thread().name}")
            process_local.session = aiohttp.ClientSession()
            global_vars.logger.debug(f"aiohttp session created successfully in process: proc-{os.getpid()}-{threading.current_thread().name}")
        return process_local

    @staticmethod
    async def close_process_resources(process_local):
        """Close process resources"""
        global_vars.logger.debug(f"Closing process resources in process: proc-{os.getpid()}-{threading.current_thread().name}")
        if hasattr(process_local, 'context'):
            global_vars.logger.debug(f"Closing Playwright context in process: proc-{os.getpid()}-{threading.current_thread().name}")
            await process_local.context.close()
        if hasattr(process_local, 'browser'):
            global_vars.logger.debug(f"Closing Playwright browser in process: proc-{os.getpid()}-{threading.current_thread().name}")
            await process_local.browser.close()
        if hasattr(process_local, 'playwright'):
            global_vars.logger.debug(f"Stopping Playwright in process: proc-{os.getpid()}-{threading.current_thread().name}")
            await process_local.playwright.stop()
        if hasattr(process_local, 'session'):
            global_vars.logger.debug(f"Closing aiohttp session in process: proc-{os.getpid()}-{threading.current_thread().name}")
            await process_local.session.close()
        global_vars.logger.debug(f"process resources closed successfully in process: proc-{os.getpid()}-{threading.current_thread().name}")

    @staticmethod
    async def fetch_with_playwright(url: str, process_local: threading.local, max_retries: int, timeout: int) -> str:
        """Fetch page content with Playwright with retry mechanism"""

        load_result = False  # Store load state result for logging as boolean
        networkidle_result = False
        content = ""

        for attempt in range(max_retries):
            page = await process_local.context.new_page()  # No user_agent here.

            try:
                global_vars.logger.debug(f"Attempt {attempt + 1}/{max_retries} for {url} in process: proc-{os.getpid()}-{threading.current_thread().name}")

                # Attempt initial load if not already successful
                if not load_result:
                    try:
                        await page.goto(url, wait_until='load', timeout=timeout * 1000)
                        load_result = True
                        content = await page.content()  # Save content on successful load
                        global_vars.logger.debug(f"Initial 'load' success for {url}")
                    except Exception as e:
                        global_vars.logger.warning(f"Initial 'load' failed for {url}: {e}")
                        raise e

                # Wait for networkidle
                try:
                    await page.wait_for_load_state('networkidle', timeout=timeout * 1000)
                    networkidle_result = True
                    content = await page.content()  # Overwrite with potentially updated content
                    global_vars.logger.debug(f"Initial 'networkidle' success for {url}")
                except Exception as e:
                    global_vars.logger.warning(f"'networkidle' wait failed for {url}: {e}")
                    raise e
                
                break
            except Exception as e:
                global_vars.logger.error(f"Error fetching {url} (attempt {attempt + 1}): {e} in process: proc-{os.getpid()}-{threading.current_thread().name}")
                if attempt == max_retries - 1:  # Last attempt failed
                    break  # Exit the loop; return "" after finally

            finally:
                await page.close()
        
        global_vars.logger.debug(f"Playwright fetch time for {url} | Load: {load_result}, NetworkIdle: {networkidle_result} in process: proc-{os.getpid()}-{threading.current_thread().name}")

        return content  # Return empty string after all retries failed

    @staticmethod
    async def process_website(provider: Dict, 
                              pipeline: CsvPipeline, 
                              max_depth: int, 
                              max_pages_per_website: int, 
                              max_concurrent_per_thread: int,
                                shutdown_event: multiprocessing.Event, batch_size: int, 
                                link_extractor: LinkExtractor, url_type_checker: URLTypeChecker,
                                timeout: int, max_retries: int):
        """Process a single website with its own queues and stats"""
        start_url = provider["website"]
        website = urlparse(start_url).netloc
        business_id = provider["businessID"]
        task_queue = queue.Queue()
        visited_urls = set()
        
        crawl_stats = {
            'start_time': time.time(),
            'total_urls': 1,
            'crawled_count': 0,
            'failed_urls': 0,
            'all_urls': {start_url}
        }

        async def crawl_page(url: str, depth: int, process_local_thread):
            """Crawl a single page"""
            start_time = time.time()


            try:
                if depth > max_depth or len(visited_urls) >= max_pages_per_website or shutdown_event.is_set():
                    global_vars.logger.debug(f"[{business_id}] Reached max depth or max pages or shutdown signal. Skipping {url} in process: proc-{os.getpid()}-{threading.current_thread().name}")
                    return

                if url in visited_urls:
                    global_vars.logger.debug(f"[{business_id}] Already visited {url}. Skipping in process: proc-{os.getpid()}-{threading.current_thread().name}")
                    return
                visited_urls.add(url)

                should_crawl = True
                html = None

                html_info = await url_type_checker.is_pdf_url_with_title(url)
                if html_info.url_type == URLType.PDF:
                    global_vars.logger.info(f"[{business_id}] Fetching pdf {html_info.url_type} URL: {url} in process: proc-{os.getpid()}-{threading.current_thread().name}")
                elif html_info.url_type == URLType.DOCX:
                    global_vars.logger.info(f"[{business_id}] Fetching doc {html_info.url_type} URL: {url} in process: proc-{os.getpid()}-{threading.current_thread().name}")
                elif html_info.url_type != URLType.HTML:
                    global_vars.logger.info(f"[{business_id}] Skipping {html_info.url_type} URL: {url} in process: proc-{os.getpid()}-{threading.current_thread().name}")
                    should_crawl = False
                else:
                    global_vars.logger.debug(f"[{business_id}] Fetching {url} with Playwright in process: proc-{os.getpid()}-{threading.current_thread().name}")
                    html = await Crawler.fetch_with_playwright(url, process_local_thread, max_retries, timeout)
                    if not html:
                        should_crawl = False
                        crawl_stats['failed_urls'] += 1
                        global_vars.logger.info(f"[{business_id}] Failed parse URL: {url} in process: proc-{os.getpid()}-{threading.current_thread().name}")
                
                crawl_stats['crawled_count'] += 1
                end_time = time.time()
                fetch_time = end_time - start_time
                global_vars.logger.info(f"[{business_id}] Crawling {url} at depth {depth} - {website} - Crawled: {crawl_stats['crawled_count']}/{crawl_stats['total_urls']} - Fetch Time: {fetch_time:.2f} seconds in process: proc-{os.getpid()}-{threading.current_thread().name}")


                if should_crawl:
                    item = {
                            'row': {
                                'url': url,
                                'content': html,
                                'title': html_info.title,
                                'processID': f"{os.getpid()}-{threading.current_thread().name}",
                                'depth': depth,
                                'url_type':  html_info.url_type,
                                **{k: provider[k] for k in ['state', 'county', 'googleReview', 
                                                    'googleReviewRating', 'googleReviewCount',
                                                    'domain', 'googleEntry', 'businessFullName', 'businessID', 'website'] if k in provider}
                            }
                        }

                    # Call pipeline to save the item
                    try:
                        if item:
                            pipeline.process_item(item, None) # Use pipeline to save to ES.
                    except Exception as e:
                        global_vars.logger.error(f"[{business_id}] Error processing item for URL {url}: {e} in process: proc-{os.getpid()}-{threading.current_thread().name}")


                    if depth < max_depth and crawl_stats['crawled_count'] < max_pages_per_website:
                        global_vars.logger.debug(f"[{business_id}] Extracting links from {url} in process: proc-{os.getpid()}-{threading.current_thread().name}")
                        try:
                            links = await link_extractor.extract_links(html, url)
                            new_links = [link for link in links if link not in visited_urls and link not in crawl_stats['all_urls']]
                            
                            crawl_stats['all_urls'].update(new_links)
                            crawl_stats['total_urls'] += len(new_links)

                            global_vars.logger.debug(f"[{business_id}] Adding {len(new_links)} new links to queue for {url} in process: proc-{os.getpid()}-{threading.current_thread().name}")
                            for i in range(0, len(new_links), batch_size):
                                batch = new_links[i:i + batch_size]
                                task_queue.put((batch, depth + 1))

                                global_vars.logger.debug(f"[{business_id}] Adding {batch} new links to queue for {url} as batch {i} in process: proc-{os.getpid()}-{threading.current_thread().name}")
                        except Exception as e:
                            global_vars.logger.error(f"[{business_id}] Error extracting or processing links from URL {url}: {e} in process: proc-{os.getpid()}-{threading.current_thread().name}")

                    else:
                        global_vars.logger.debug(f"[{business_id}] Max depth or max pages reached, not extracting links from {url} in process: proc-{os.getpid()}-{threading.current_thread().name}")
                else:
                    global_vars.logger.debug(f"[{business_id}] Skipping link extraction for {url} in process: proc-{os.getpid()}-{threading.current_thread().name}")

            except Exception as e:
                global_vars.logger.error(f"[{business_id}] An unexpected error occurred while crawling URL {url}: {e} in process: proc-{os.getpid()}-{threading.current_thread().name}")


        async def thread_worker():
            """Worker that processes URLs from the queue"""
            global_vars.logger.debug(f"Thread worker started in thread: proc-{os.getpid()}-{threading.current_thread().name}")

            process_local_thread = threading.local()
            process_local_thread = await Crawler.init_process_resources()

            try:
                while not shutdown_event.is_set():
                    try:
                        batch, depth = task_queue.get(timeout=1.0)
                        global_vars.logger.debug(f"Got batch of {len(batch)} URLs from queue at depth {depth} in thread: proc-{os.getpid()}-{threading.current_thread().name}")
                        tasks = [crawl_page(url, depth, process_local_thread) for url in batch]
                        global_vars.logger.debug(f"Creating {len(tasks)} crawl tasks for batch in thread: proc-{os.getpid()}-{threading.current_thread().name}")
                        await asyncio.gather(*tasks, return_exceptions=True)
                        task_queue.task_done()
                        global_vars.logger.debug(f"Batch processing completed in thread: proc-{os.getpid()}-{threading.current_thread().name}")
                    except queue.Empty:
                        # Check if we should exit
                        if crawl_stats['crawled_count'] >= crawl_stats['total_urls'] or crawl_stats['crawled_count'] >= max_pages_per_website:
                            global_vars.logger.debug(f"Crawled all URLs or reached max pages, exiting thread worker in thread: proc-{os.getpid()}-{threading.current_thread().name}")
                            break
                        global_vars.logger.debug(f"Queue is empty, continuing in thread: proc-{os.getpid()}-{threading.current_thread().name} {crawl_stats['crawled_count']} / {crawl_stats['total_urls']}")
                        continue
            except Exception as e:
                global_vars.logger.error(f"Thread worker error for {website}: {e} in thread: proc-{os.getpid()}-{threading.current_thread().name}")
            
            await Crawler.close_process_resources(process_local_thread)

        # Start with initial URL
        global_vars.logger.info(f"[{business_id}] Starting crawl for {start_url} in process: proc-{os.getpid()}-{threading.current_thread().name}")
        task_queue.put(([start_url], 0))

        # Create thread pool for this website
        with ThreadPoolExecutor(max_workers=max_concurrent_per_thread) as executor:
            loop = asyncio.get_event_loop()
            tasks = []
            
            for _ in range(max_concurrent_per_thread):
                task = loop.run_in_executor(
                    executor,
                    lambda: asyncio.new_event_loop().run_until_complete(thread_worker())
                )
                tasks.append(task)
            
            try:
                global_vars.logger.info(f"[{business_id}] Waiting for thread workers to complete for {website} in process: proc-{os.getpid()}-{threading.current_thread().name}")
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout * 100)
            except asyncio.TimeoutError:
                global_vars.logger.warning(f"[{business_id}] Timeout reached for website {website} in process: proc-{os.getpid()}-{threading.current_thread().name}")
            
            finally:
                global_vars.logger.info(f"[{business_id}] Cancelling remaining tasks for {website} in process: proc-{os.getpid()}-{threading.current_thread().name}")
                for task in tasks:
                    if not task.done():
                        task.cancel()

        # Print stats for this website
        end_time = time.time()
        total_time = end_time - crawl_stats['start_time']
        average_time = total_time / crawl_stats['crawled_count'] if crawl_stats['crawled_count'] else 0

        global_vars.logger.info(f"""\n[{business_id}] Crawling statistics for {website}:"
        "  [{business_id}] Total time: {total_time:.2f} seconds"
        "  [{business_id}] Total URLs crawled: {crawl_stats['crawled_count']}"
        "  [{business_id}] Failed URLs: {crawl_stats['failed_urls']}"
        "  [{business_id}] Total URLs found: {crawl_stats['total_urls']}"
        "  [{business_id}] Average time per page: {average_time:.2f} seconds""")

    @staticmethod
    def website_worker(start_providers_queue: multiprocessing.Queue, shutdown_event: multiprocessing.Event,
                       max_depth: int, 
                       max_pages_per_website: int, 
                       max_concurrent_per_thread: int,
                       batch_size: int, 
                       timeout: int, max_retries: int):
        """Worker that processes websites from the start_providers_queue"""
        # Initialize the pipeline here, so each website worker has its own instance
        pipeline = CsvPipeline()
        link_extractor = LinkExtractor()
        url_type_checker = URLTypeChecker()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        global_vars.logger.info(f"Website worker started in process: proc-{os.getpid()}-{threading.current_thread().name}")

        while not shutdown_event.is_set():
            try:
                provider = start_providers_queue.get(timeout=1)  # Changed from get_nowait to get with timeout
                global_vars.logger.info(f"[{provider['businessID']}] Got website {provider['website']} from queue in process: proc-{os.getpid()}-{threading.current_thread().name}")
                loop.run_until_complete(Crawler.process_website(provider, pipeline, 
                                                                max_depth, 
                                                                max_pages_per_website,
                                                                max_concurrent_per_thread,
                                                                
                                                        shutdown_event, batch_size, link_extractor, url_type_checker,
                                                        timeout, max_retries))
                start_providers_queue.task_done()
                global_vars.logger.info(f"[{provider['businessID']}] Finished processing website {provider['website']} in process: proc-{os.getpid()}-{threading.current_thread().name}")
            except queue.Empty:
                global_vars.logger.info(f"Start URL queue is empty, exiting website worker in process: proc-{os.getpid()}-{threading.current_thread().name}")
                break
            except Exception as e:
                global_vars.logger.error(f"Error in website worker: {e} in process: proc-{os.getpid()}-{threading.current_thread().name}")

        loop.close()

    def crawl_website(self, start_providers: List[Dict]):
        """Main crawl method"""
        global_vars.logger.info("Starting crawl process")
        self.shutdown_event.clear()

        # Create a multiprocessing.Manager to create shared queue and event
        manager = multiprocessing.Manager()
        start_providers_queue = manager.Queue()
        shutdown_event = manager.Event()

        # Add all start URLs to the queue
        for provider in start_providers:
            start_providers_queue.put(provider)
            global_vars.logger.info(f"[{provider['businessID']}] Added {provider['website']} to start URL queue")

        # Create process pool
        with multiprocessing.Pool(processes=self.max_processes) as pool:
            # Prepare arguments for the worker function
            worker_args = (start_providers_queue, shutdown_event,
                           self.max_depth, 
                           self.max_pages_per_website, 
                           self.max_concurrent_per_thread,
                           self.batch_size,
                           self.timeout, self.max_retries)

            # Use the pool to execute the worker function for each process
            arg_list = [worker_args] * self.max_processes
            pool.starmap(Crawler.website_worker, arg_list)

        global_vars.logger.info("Crawl process finished")