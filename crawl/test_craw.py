import base64
import hashlib
import io
import mimetypes
import os
import pickle
import sys
import time
import traceback
import uuid

import concurrent.futures
from bs4 import BeautifulSoup, Comment
import pandas as pd
import requests
sys.path.append("../")
import asyncio
from crawl4ai import AsyncWebCrawler, CacheMode, CrawlerRunConfig, LXMLWebScrapingStrategy
from crawl4ai.content_filter_strategy import PruningContentFilter, BM25ContentFilter
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher
import json
from service.llm import call_llm, call_gemini, count_tokens, GeminiLLMCaller
from crawl4ai import CrawlerMonitor, DisplayMode
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer
from crawl4ai.deep_crawling import BestFirstCrawlingStrategy
import uuid

from crawl4ai import RateLimiter
import global_vars
from service.utils.common_utils import is_file_url

SUMMARY = False
CACHE_DIR = "crawl_cache"
# MODEL_NAME = 'gemini-2.5-pro-exp-03-25'
MODEL_NAME = 'gemini-2.0-flash-lite'
# MODEL_NAME = 'gemini-2.0-flash-thinking-exp-01-21'
# MODEL_NAME = 'gemini-2.5-pro-exp-03-25'
PROMPT_PATH = '/home/azureuser/Trustbeez/prompt/web_prompt_summer.md'


    

def filter_html(html):
    soup = BeautifulSoup(html, 'lxml')
    if not soup.body:
        soup = BeautifulSoup(f"<body>{html}</body>", "lxml")
    
    # 删除所有注释
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.decompose()  # 或者使用 comment.decompose()
    
    excluded_tags = [
        "nav",
        "footer",
        "header",
        # "aside",
        "script",
        "br"
        # "style",
        # "form",
        # "iframe",
        # "noscript",
    ]
    
    for tag in excluded_tags:
        for element in soup.find_all(tag):
            element.decompose()
            
    body = soup.find("body")            

    # 提取图片信息并保存到列表
    images = []
    for img in body.find_all('img'):
        if 'src' in img.attrs:  # 确保<img>标签有src属性
            # print(img.attrs)
            img_url = img['src']
            
            # 下载图片
            # response = requests.get(img_url)
            # if response.status_code == 200:
            #     # 将图片内容转换为 base64 编码
            #     image_data = response.content
            #     from PIL import Image
            #     try:
            #         image = Image.open(io.BytesIO(image_data))
            #         image_format = image.format.lower()
                    
            #         if image_format in ['jpg', 'jpeg', 'png', 'bmp']:
            #             base64_image = base64.b64encode(image_data).decode()
            #             base64_url = f"data:image/{image_format};base64,{base64_image}"
                        
            #             images.append(base64_url)
            #     except:
            #         pass
            
            # img_name = img.get('alt', '')
            # if not img_name:
            #     img_name = img.get('title', '')
            images.append(img_url)
        img.decompose()

    return str(body), images

async def crawl_website_all_links(base_url, level=3):
    
    content_filter = PruningContentFilter(
        threshold=0.35,
        # "fixed" or "dynamic"
        # threshold_type="dynamic",
        # Ignore nodes with <5 words
        # min_word_threshold=3
    )
    md_generator = DefaultMarkdownGenerator(content_filter = content_filter)
    # md_generator = DefaultMarkdownGenerator()
    config = CrawlerRunConfig(
        markdown_generator=md_generator,
        check_robots_txt=True,
        # page_timeout=30000,
        user_agent='Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Mobile Safari/537.36',
        # cache_mode=CacheMode.ENABLED,
        # cache_mode=CacheMode.DISABLED,
        cache_mode=CacheMode.WRITE_ONLY
    )
    dispatcher = MemoryAdaptiveDispatcher(
        check_interval=0,
        rate_limiter=RateLimiter(
            base_delay=(0, 0),
            max_delay=0
        ),
        max_session_permit=32
        # monitor=CrawlerMonitor(
        #     display_mode=DisplayMode.DETAILED
        # )
    )
    visited_urls = set()
    urls_to_visit = set()
    urls_to_visit.add(base_url)
    count = 0
    seen_contents = set()
    pages = []
    all_images = []
    async with AsyncWebCrawler() as crawler:
        while urls_to_visit:
            count += 1
            if count == level + 1:
                break
            
            # print(len(visited_urls))
            results = await crawler.arun_many(
                urls=list(urls_to_visit),
                config=config,
                dispatcher=dispatcher,
            )

            for url in urls_to_visit:
                visited_urls.add(url)
            urls_to_visit.clear()
            
            for result in results:
                if not result or not result.html:
                    continue
                internal_links = result.links.get("internal", [])
                if count == 1:
                    content = result.markdown.raw_markdown
                else:
                    # content = result.markdown.fit_markdown
                    filter_content, images = filter_html(result.html)
                    # all_images.extend(images)
                    content = md_generator.generate_markdown(filter_content).raw_markdown
                # content = result.markdown
                if content not in seen_contents:
                    # if content.lower().find('summer') >= 0 or content.lower().find('camp') >= 0:
                    pages.append({
                        'content': content,
                        'url': result.url,
                        'title': result.metadata.get('title', '')
                    })
                    seen_contents.add(content)
                    for link in internal_links:
                        
                        if len(urls_to_visit) > 100:
                            break
                        if link['href'] not in visited_urls:
                            urls_to_visit.add(link['href'])

    concat_content = ''
    for idx, page in enumerate(pages):
        if idx == 0:
            separator = f"\n\n---\nHomepage: {page['title']}: {page['url']}\n---\n\n"
        else:
            # 添加显性分隔符
            separator = f"\n\n---\nPage {idx}: {page['title']}: {page['url']}\n---\n\n"
        concat_content += separator + page['content']
    return concat_content, len(pages), all_images

async def crawl_test():
    # await crawl_website_deep('https://www.creatif.com/livingston-nj/camp-calendar/')
    concat_content, _, _ = await crawl_website_all_links('https://www.creatif.com/livingston-nj/camp-calendar/', level=3)
    with open('test.md', 'w', encoding='utf-8') as f:
        f.write(concat_content)
async def main():
    await crawl_test()

    
if __name__ == "__main__":
    global_vars.init_globals()
    asyncio.run(main())
    
