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

from enum import Enum

class URLType(Enum):
    HTML = 0
    PDF = 1
    DOCX = 2
    OTHER = 3
    ERROR = -1

class URLTypeChecker:
    """
    Checks the type of a URL (HTML, PDF, DOCX, etc.) based on headers and suffix.
    """

    async def is_pdf_url(self, url: str) -> URLType:
        """Checks if a URL points to a PDF by inspecting Content-Type header or URL suffix."""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
                'Accept': 'text/html,application/pdf,*/*;q=0.9',  # 明确声明可以接受 PDF
            }
            # 先检查 URL 后缀（快速判断，避免网络请求）
            url_lower = url.lower()
            if url_lower.endswith('.pdf'):
                return URLType.PDF
            if url_lower.endswith('.docx'):
                return URLType.DOCX
            
            # 发送  请求（只获取头部，stream=True 避免下载内容）
            response = requests.get(  # 使用 HEAD 请求
                url,
                headers=headers,
                stream=True,  # 不下载内容，只获取头
                allow_redirects=True,
                timeout=5,
            )

            # 确保 HTTP 请求成功
            response.raise_for_status()

            # 获取 Content-Type
            content_type = response.headers.get('Content-Type', '').lower()

            # 显式关闭连接（避免未关闭的连接）
            response.close()

            # 检查是否是 PDF
            if 'text/html' in content_type:
                return URLType.HTML
            elif 'application/pdf' in content_type:
                return URLType.PDF
            elif 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in content_type:
                return URLType.DOCX
            else:
                return URLType.OTHER  # Likely a webpage or HTML content

        except requests.exceptions.RequestException as e:
            print(f"Error checking {url}: {e}")  # 调试信息（可选）
            return URLType.ERROR


        except Exception as e:
            print(f"Unexpected error checking {url}: {e}")
            return URLType.ERROR

        finally:
            # Ensure the response is closed, even in case of errors
            if 'response' in locals():  # Check if the variable exists
                try:
                    response.close()
                except Exception as e:
                    print(f"Error closing response for {url}: {e}")

