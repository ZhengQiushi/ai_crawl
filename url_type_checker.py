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
import global_vars
class URLType(Enum):
    HTML = 0
    PDF = 1
    DOCX = 2
    OTHER = 3
    ERROR = -1

class URLInfo:
    def __init__(self, url_type: URLType, title: str = None, response: requests.Response = None):
        self.url_type = url_type
        self.title = title
        self.response = response

    def __repr__(self):
        return f"URLInfo(type={self.url_type}, title='{self.title}')"

class URLTypeChecker:

    async def is_pdf_url_with_title(self, url: str, max_retries: int = 5, retry_delay: int = 2) -> URLInfo:
        """Checks if a URL points to a PDF or HTML page, and returns the URLType and title (if HTML).
        Handles retries for 429 errors.
        """

        for attempt in range(max_retries):
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

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    global_vars.logger.info(f"Received 429 error for {url}, retrying in {retry_delay} seconds (attempt {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1: # Only sleep if there are retries left
                        await asyncio.sleep(retry_delay)
                        continue
                    else:
                        global_vars.logger.error(f"Max retries reached for {url} after receiving 429 errors.  Returning ERROR.")
                        return URLInfo(URLType.ERROR) # Give up after max retries

                else:
                    global_vars.logger.error(f"HTTP error checking {url}: {e}")
                    return URLInfo(URLType.ERROR)

            except requests.exceptions.RequestException as e:
                global_vars.logger.error(f"Error checking {url}: {e}")  # 调试信息（可选）
                return URLInfo(URLType.ERROR)


            except Exception as e:
                global_vars.logger.error(f"Unexpected error checking {url}: {e}")
                return URLInfo(URLType.ERROR)

            finally:
                # Ensure the response is closed, even in case of errors
                if 'response' in locals():  # Check if the variable exists
                    try:
                        response.close()
                    except Exception as e:
                        global_vars.logger.error(f"Error closing response for {url}: {e}")
            # If the request was successful (no exceptions raised), exit the loop
            break

        return URLInfo(URLType.ERROR)  # Should not reach here if successful on first try, or retries are exhausted due to 429.

async def main():
    base_url = "http://www.centerstagenj.co"
    extractor = URLTypeChecker()
    url_info = await extractor.is_pdf_url_with_title(base_url)
    print(base_url, url_info.response.url)


if __name__ == "__main__":
    global_vars.init_globals("/root/ai_crawl/config.env")
    asyncio.run(main())