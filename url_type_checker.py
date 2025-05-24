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

class URLInfo:
    def __init__(self, url_type: URLType, title: str = None):
        self.url_type = url_type
        self.title = title

    def __repr__(self):
        return f"URLInfo(type={self.url_type}, title='{self.title}')"


class URLTypeChecker:
    """
    Checks the type of a URL (HTML, PDF, DOCX, etc.) based on headers and suffix.
    """
    async def is_pdf_url_with_title(self, url: str) -> URLInfo:
        """Checks if a URL points to a PDF or HTML page, and returns the URLType and title (if HTML)."""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
                'Accept': 'text/html,application/pdf,*/*;q=0.9',  # 明确声明可以接受 PDF
            }
            # 先检查 URL 后缀（快速判断，避免网络请求）
            url_lower = url.lower()
            if url_lower.endswith('.pdf'):
                return URLInfo(URLType.PDF)  # No title for PDF
            if url_lower.endswith('.docx'):
                return URLInfo(URLType.DOCX)  # No title for DOCX

            # 发送  请求（只获取头部，stream=True 避免下载内容）
            response = requests.get(  # 使用 GET 请求 to get content for title
                url,
                headers=headers,
                stream=True,  # initially, don't download the content
                allow_redirects=True,
                timeout=5,
            )

            # 确保 HTTP 请求成功
            response.raise_for_status()

            # 获取 Content-Type
            content_type = response.headers.get('Content-Type', '').lower()

            # 检查是否是 PDF
            if 'text/html' in content_type:
                # Get the title
                response.raw.decode_content = True  # Handle gzipped content
                soup = BeautifulSoup(response.content, 'html.parser')
                title = soup.title.string if soup.title else None
                response.close()
                return URLInfo(URLType.HTML, title=title)
            elif 'application/pdf' in content_type:
                response.close()
                return URLInfo(URLType.PDF)
            elif 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' in content_type:
                response.close()
                return URLInfo(URLType.DOCX)
            else:
                response.close()
                return URLInfo(URLType.OTHER)  # Likely a webpage or HTML content

        except requests.exceptions.RequestException as e:
            print(f"Error checking {url}: {e}")  # 调试信息（可选）
            return URLInfo(URLType.ERROR)


        except Exception as e:
            print(f"Unexpected error checking {url}: {e}")
            return URLInfo(URLType.ERROR)

        finally:
            # Ensure the response is closed, even in case of errors
            if 'response' in locals():  # Check if the variable exists
                try:
                    response.close()
                except Exception as e:
                    print(f"Error closing response for {url}: {e}")