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




class LinkExtractor:
    excluded_extensions = ['.xls', '.xlsx', '.ppt', '.pptx',
                            '.zip', '.rar', '.jpg', '.jpeg', '.png', '.gif', '.mp3',
                            '.mp4', '.avi', '.mov', '.ics', '.ical']

    async def extract_links(self, html: str, base_url: str) -> List[str]:
        """从HTML中提取链接，过滤指定扩展名，并且只返回子域名链接"""
        soup = BeautifulSoup(html, 'html.parser')
        links = []
        base_domain = urlparse(base_url).netloc

        for a in soup.find_all('a', href=True):
            href = a['href']
            absolute_url = urljoin(base_url, href)
            parsed = urlparse(absolute_url)

            # 检查协议和子域名
            link_domain = parsed.netloc
            if link_domain.endswith(base_domain):  # 确保是子域名或相同域名

                # 检查是否为排除的扩展名
                if not any(absolute_url.lower().endswith(ext) for ext in self.excluded_extensions):
                    links.append(absolute_url)

        return links
