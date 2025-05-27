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
from w3lib import html

class LinkExtractor:
    excluded_extensions = ['.xls', '.xlsx', '.ppt', '.pptx',
                            '.zip', '.rar', '.jpg', '.jpeg', '.png', '.gif', '.mp3',
                            '.mp4', '.avi', '.mov', '.ics', '.ical']

    def myurlparse(self, url: str) -> Optional[str]:
        """解析URL，返回域名或None"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
            if domain.startswith('www.'):
                domain = domain[4:]  # 移除www前缀
            return domain
        except Exception as e:
            logging.error(f"Error parsing URL {url}: {e}")
            return None

    async def extract_links(self, html: str, base_url: str) -> List[str]:
        """从HTML中提取链接，过滤指定扩展名，并且只返回子域名链接，并去重"""
        soup = BeautifulSoup(html, 'html.parser')
        links = set()  # 使用集合来去重

        for a in soup.find_all('a', href=True):
            href = a['href']
            absolute_url = urljoin(base_url, href)

            if absolute_url.find("docs.google.com/forms") != -1:
                links.add(absolute_url)
                continue

            # 检查协议和子域名
            base_domain = self.myurlparse(base_url)
            link_domain = self.myurlparse(absolute_url)
            if link_domain.find(base_domain) != -1 or base_domain.find(link_domain) != -1:  # 确保是子域名或相同域名
                # 检查是否为排除的扩展名
                if not any(absolute_url.lower().find(ext) != -1 for ext in self.excluded_extensions):
                    links.add(absolute_url)

        return list(links)  # 返回列表


async def main():
    html_file_path = "/root/ai_crawl/data/html.txt"
    base_url = "http://www.centerstagenj.com/summer-programs"

    try:
        with open(html_file_path, "r", encoding="utf-8") as f:
            html_content = f.read()
    except FileNotFoundError:
        print(f"Error: File not found at {html_file_path}")
        return
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    extractor = LinkExtractor()
    links = await extractor.extract_links(html_content, base_url)

    print("Extracted Links:")
    for link in links:
        print(link)

if __name__ == "__main__":
    asyncio.run(main())