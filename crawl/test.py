import hashlib
import os
import pickle
import pstats
import sys
import time
import uuid

import concurrent.futures
import pandas as pd
import requests
sys.path.append("../")
import asyncio
from crawl4ai import AsyncWebCrawler, CacheMode, CrawlerRunConfig
from crawl4ai.content_filter_strategy import PruningContentFilter, BM25ContentFilter
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher
import json
from service.llm import call_llm, call_gemini, count_tokens
from crawl4ai import CrawlerMonitor, DisplayMode
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer
from crawl4ai.deep_crawling import BestFirstCrawlingStrategy


from crawl4ai import RateLimiter
import logging
from bs4 import BeautifulSoup, Comment
async def crawl_website_all_links(base_url, level=1, raw=False, threshold=0.48):
    
    content_filter = PruningContentFilter(
        threshold=threshold,
        # "fixed" or "dynamic"
        # threshold_type="dynamic",
        # Ignore nodes with <5 words
        # min_word_threshold=10
    )
    md_generator_filter = DefaultMarkdownGenerator(content_filter = content_filter)
    md_generator = DefaultMarkdownGenerator()
    config = CrawlerRunConfig(
        markdown_generator=md_generator_filter,
        check_robots_txt=True,
        page_timeout=30000
        # cache_mode=CacheMode.ENABLED,
        # cache_mode=CacheMode.DISABLED,
        # cache_mode=CacheMode.WRITE_ONLY
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
    image_count = 0
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
                if not result or not result.markdown:
                    continue
                internal_links = result.links.get("internal", [])
                images = result.media.get("images", [])
                image_count += len(images)
                
                
                if raw:
                    content = result.markdown.fit_markdown
                else:
                    content = filter_html(result.html)
                    content = md_generator.generate_markdown(content).raw_markdown
                    # content = result.markdown.raw_markdown
                    # content = result.markdown.fit_markdown

                # content = result.markdown
                if content not in seen_contents:
                    pages.append({
                        'content': content,
                        'url': result.url,
                        'title': result.metadata.get('title', '')
                    })
                    seen_contents.add(content)
                        
                    for link in internal_links:
                        if len(urls_to_visit) > 100:
                            break
                        if link['href'] not in visited_urls and (link['text'] or link['title']) and not is_file_url(link['href']):
                            urls_to_visit.add(link['href'])

    # print(image_count)
    concat_content = ''
    for idx, page in enumerate(pages):
        # 添加显性分隔符
        separator = f"\n\n---\nPage {idx}: {page['title']}: {page['url']}\n---\n\n"
        concat_content += separator + page['content']
    return concat_content, len(pages)


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
        "br"
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
            print(img_url)
            if img_url.find('base64') >= 0:
                img.decompose()
                continue
            # img_name = img.get('alt', '')
            # if not img_name:
            #     img_name = img.get('title', '')
            images.append(img_url)
        img.decompose()

    return str(body), images
def test_markdown():
    # print(count_tokens("https://centercourtacademy.com/sports/summer-camps/"))
    content_filter = PruningContentFilter(
        threshold=0.35,
        # "fixed" or "dynamic"
        # threshold_type="dynamic",
        # Ignore nodes with <5 words
        # min_word_threshold=10
    )
    # md_generator = DefaultMarkdownGenerator()
    md_generator_filter = DefaultMarkdownGenerator(content_filter=content_filter)
    # url = 'https://www.njballet.org/open-youth-intensive-2025'
    url = 'https://parkridgetennisacademy.com/summer-camp'
    # url = 'https://usatravelfun.com/summer-camp/'
    response = requests.get(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    })
    print(response)
    if response.status_code == 200:
        
        
        
        
        # 获取剩余的内容
        remaining_content, images = filter_html(response.text)
        print(images)
        markdown = md_generator_filter.generate_markdown(remaining_content)
        # markdown = md_generator_filter.generate_markdown(remaining_content)
        # print(markdown)
        print(markdown.raw_markdown)
        # print(json.dumps(images, indent=4))


async def main():
    target = 'https://www.njballet.org/'
    
    concat_content, page_count = await crawl_website_all_links(target, 3, raw=True, threshold=0.35)
    with open("output/test_raw.md", "w") as f:
        f.write(concat_content)
    raw_len = len(concat_content)
    
    concat_content, page_count = await crawl_website_all_links(target, 3, raw=False, threshold=0.35)
    with open("output/test_fit.md", "w") as f:
        f.write(concat_content)
    fit_len = len(concat_content)
    
    print(f"raw: {raw_len}, fit: {fit_len}, radio: {fit_len / (raw_len+1)}")
if __name__ == "__main__":
    # asyncio.run(main())
    test_markdown()
    # cProfile.run('test_markdown()', 'profile_stats')
    # stats = pstats.Stats('profile_stats')
    # stats.sort_stats('tottime')  # 按内部耗时排序（不包括子函数）
    # stats.sort_stats('cumtime')  # 按累计耗时排序（包括子函数）
    # stats.print_stats()
    
    # test = geocode('299 Columbia Turnpike, Florham Park, NJ 07932')
    
    # print(test)