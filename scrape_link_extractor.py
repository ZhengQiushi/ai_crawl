import asyncio
from urllib.parse import urlparse, urljoin
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.url import canonicalize_url
from bs4 import BeautifulSoup
import logging
from typing import List, Set, Dict, Optional
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.http import TextResponse


class AsyncLinkExtractor:
    def __init__(self):
        # Initialize Scrapy's LinkExtractor with appropriate settings
        self.link_extractor = LinkExtractor(
            deny_extensions=[
                'xls', 'xlsx', 'ppt', 'pptx', 'zip', 'rar', 
                'jpg', 'jpeg', 'png', 'gif', 'mp3', 'mp4', 
                'avi', 'mov', 'ics', 'ical'
            ],
            canonicalize=True,
            unique=True
        )
        self.excluded_extensions = [
            'xls', 'xlsx', 'ppt', 'pptx', 'zip', 'rar',
            'jpg', 'jpeg', 'png', 'gif', 'mp3', 'mp4',
            'avi', 'mov', 'ics', 'ical'
        ]

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
        
    async def extract_links(self, scrapy_like_response: TextResponse, base_url: str) -> List[str]:
        """Extract links from HTML using Scrapy's LinkExtractor"""
        # Parse HTML with BeautifulSoup first to get proper HTML structure
        link_extractor = LxmlLinkExtractor(unique=True)     
        links = link_extractor.extract_links(scrapy_like_response)

        
        # Filter links to only include same domain or subdomains
        extracted_urls = set()  # 使用集合来去重
        for link in links:
            absolute_url = urljoin(base_url, link.url)
            
            if "docs.google.com/forms" in absolute_url:
                extracted_urls.add(absolute_url)
                continue

            # 检查协议和子域名
            base_domain = self.myurlparse(base_url)
            link_domain = self.myurlparse(absolute_url)

            if base_domain and link_domain and (base_domain in link_domain or link_domain in base_domain):
                # 检查是否为排除的扩展名
                is_ok = True
                for ext in self.excluded_extensions:
                    if absolute_url.lower().endswith(ext):
                        is_ok = False
                        break
                if is_ok:
                    extracted_urls.add(absolute_url)

        return list(extracted_urls)  # 返回列表


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

    scrapy_like_response = TextResponse(
        url=base_url,
        body=html_content, 
        encoding='utf-8'
    )

    extractor = AsyncLinkExtractor()
    links = await extractor.extract_links(scrapy_like_response, base_url)

    print("Extracted Links:")
    for link in links:
        print(link)

if __name__ == "__main__":
    asyncio.run(main())