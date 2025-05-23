import sys

import pandas as pd
sys.path.append("../")
import asyncio
from crawl4ai import AsyncWebCrawler, CacheMode, CrawlerRunConfig
from crawl4ai.content_filter_strategy import PruningContentFilter, BM25ContentFilter
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher
import json
from service.llm import call_llm, call_gemini
from utils import extract_json_string, is_file_url, read_file
from crawl4ai import CrawlerMonitor, DisplayMode
from crawl4ai import RateLimiter

SUMMARY = False

def parsing(context):
    prompt = read_file('../prompt/web_prompt_summer.md')
    
    messages = [
        {
            "role": "system",
            "content": prompt
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": context
                }
            ]
        }
    ]
    rsp = call_llm(messages, source='gemini')
    messages.append({
        "role": "assistant",
        "content": rsp
    })
    
    if SUMMARY:
        with open('../prompt/summary.md') as f:
            summary_template = f.read()
        # summary
        messages.append({
            "role": "user",
            "content": summary_template
        })
        rsp = call_llm(messages, source='gemini')
    
    
    return rsp

async def crawl_website_all_links(base_url, level=2):
    
    content_filter = PruningContentFilter(
        # threshold=0.45,
        # "fixed" or "dynamic"
        # threshold_type="dynamic",
        # Ignore nodes with <5 words
        # min_word_threshold=3
    )
    md_generator = DefaultMarkdownGenerator(content_filter = content_filter)
    config = CrawlerRunConfig(
        markdown_generator=md_generator,
        page_timeout=15000,
        check_robots_txt=True
        # cache_mode=CacheMode.DISABLED,
        # cache_mode=CacheMode.WRITE_ONLY
    )
    dispatcher = MemoryAdaptiveDispatcher(
        check_interval=0,
        rate_limiter=RateLimiter(
            base_delay=(0, 0),
            max_delay=0
        ),
        max_session_permit=64
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
                if not result or not result.markdown_v2:
                    continue
                internal_links = result.links.get("internal", [])
                content = result.markdown_v2.fit_markdown
                # content = result.markdown
                if content in seen_contents:
                    print(f"Duplicate content found, skipping...")
                else:
                    pages.append({
                        'content': content,
                        'url': result.url,
                        'title': result.metadata.get('title', '')
                    })
                    seen_contents.add(content)
                # print(json.dumps(internal_links, indent=4))
                for link in internal_links:
                    if link['href'] not in visited_urls and link['text'] and not is_file_url(link['href']):
                        urls_to_visit.add(link['href'])
                        if len(urls_to_visit) > 100:
                            break  

    concat_content = ''
    for idx, page in enumerate(pages):
        # 添加显性分隔符
        separator = f"\n\n---\nPage {idx}: {page['title']}: {page['url']}\n---\n\n"
        concat_content += separator + page['content']
    return concat_content, len(pages)

async def process(idx, row):
    page_count = 0
    content = ''
    content, page_count = await crawl_website_all_links(row['Website'], level=3)
    # print('parse')
    # parsing_result = parsing(content)
    # parsing_result = extract_json_string(parsing_result)
    # return parsing_result

async def main():
    data = pd.read_csv('data/google_schools_Essex.csv', nrows=None)
    data = data.fillna('')  # 将空值替换为空字符串
    

    results = []
    data['parsing'] = ''
    for idx, row in data.iterrows():
        print(idx ,row['Name'])
        result = await process(idx, row)
        data.at[idx, 'parsing'] = result
        
        print(f"Processed {idx+1}/{len(data)}")
    
    # data = data[['Website', 'parsing']]
    # data.to_excel("output/results_week1.xlsx", index=False)
    # data.to_csv("output/results_week1.csv", index=False)
    # print("All results have been saved")
    
if __name__ == "__main__":
    asyncio.run(main())