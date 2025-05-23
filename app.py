import asyncio
import aiohttp
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
import time
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AsyncWebCrawler:
    def __init__(self, start_url, max_depth=2, concurrent_requests_per_domain=2, max_tasks=100):
        self.start_url = start_url
        self.max_depth = max_depth
        self.concurrent_requests_per_domain = concurrent_requests_per_domain
        self.max_tasks = max_tasks  # 最大任务数，防止内存溢出
        self.visited_urls = set()
        self.task_queue = asyncio.Queue()
        self.domain_semaphores = {}
        self.session = None  # aiohttp session
        self.executor = ThreadPoolExecutor(max_workers=10)  # 用于解析HTML的线程池
        self.total_urls_discovered = 0
        self.total_urls_visited = 0

    async def initialize(self):
        """初始化 aiohttp session"""
        self.session = aiohttp.ClientSession()
        await self.task_queue.put((self.start_url, 0))  # 初始 URL 和深度 0
        self.visited_urls.add(self.start_url)

    async def close(self):
        """关闭 aiohttp session"""
        if self.session:
            await self.session.close()
        self.executor.shutdown(wait=True)

    def get_domain(self, url):
        """提取域名"""
        return urllib.parse.urlparse(url).netloc

    async def fetch_url(self, url, depth):
        """抓取 URL 内容"""
        domain = self.get_domain(url)
        if domain not in self.domain_semaphores:
            self.domain_semaphores[domain] = asyncio.Semaphore(self.concurrent_requests_per_domain)

        async with self.domain_semaphores[domain]:
            try:
                async with self.session.get(url, timeout=10) as response:
                    if response.status == 200:
                        self.total_urls_visited += 1
                        logging.info(f"抓取成功: {url} (深度: {depth}, 已访问: {self.total_urls_visited}, 已发现: {self.total_urls_discovered})")
                        return await response.text()
                    else:
                        logging.warning(f"抓取失败: {url} (状态码: {response.status})")
                        return None
            except Exception as e:
                logging.error(f"抓取 {url} 发生异常: {e}")
                return None

    def parse_html(self, html, current_url, depth):
        """解析 HTML，提取链接"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            links = []
            for a_tag in soup.find_all('a', href=True):
                link = a_tag['href']
                absolute_url = urllib.parse.urljoin(current_url, link)
                if absolute_url.startswith(self.start_url) and absolute_url not in self.visited_urls: # 只爬取起始 URL 域名下的链接
                    links.append(absolute_url)
                    self.visited_urls.add(absolute_url)  # 立即添加到已访问集合
                    self.total_urls_discovered += 1

            return links
        except Exception as e:
            logging.error(f"解析 HTML 发生异常: {e}")
            return []

    async def process_url(self, url, depth):
        """处理单个 URL"""
        if depth > self.max_depth or len(self.visited_urls) > self.max_tasks:
            return

        html = await self.fetch_url(url, depth)
        if html:
            # 使用线程池执行 HTML 解析
            links = await asyncio.get_event_loop().run_in_executor(
                self.executor, self.parse_html, html, url, depth
            )

            for link in links:
                await self.task_queue.put((link, depth + 1))

    async def crawl(self):
        """主爬取循环"""
        while not self.task_queue.empty():
            url, depth = await self.task_queue.get()
            await self.process_url(url, depth)
            self.task_queue.task_done()

    async def run(self):
        """启动爬虫"""
        start_time = time.time()
        await self.initialize()
        logging.info(f"爬虫开始，起始 URL: {self.start_url}, 最大深度: {self.max_depth}, 每个域名并发数: {self.concurrent_requests_per_domain}")

        try:
            await asyncio.gather(*(self.crawl() for _ in range(10)))  # 启动多个爬取任务
            await self.task_queue.join()  # 等待所有任务完成
        finally:
            await self.close()
            end_time = time.time()
            logging.info(f"爬虫结束，总共发现 {self.total_urls_discovered} 个链接, 访问了 {self.total_urls_visited} 个链接, 耗时: {end_time - start_time:.2f} 秒")

# 示例用法
async def main():
    crawler = AsyncWebCrawler("http://creatif.com", max_depth=3, concurrent_requests_per_domain=128, max_tasks=500)
    await crawler.run()

if __name__ == "__main__":
    asyncio.run(main())