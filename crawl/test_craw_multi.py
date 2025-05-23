import hashlib
import os
import pickle
import sys
import time
import traceback
import uuid

import concurrent.futures
import pandas as pd
sys.path.append("../")
import asyncio
from crawl4ai import AsyncWebCrawler, CacheMode, CrawlerRunConfig
from crawl4ai.content_filter_strategy import PruningContentFilter, BM25ContentFilter
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher
import json
from service.llm import call_llm, call_gemini, count_tokens, GeminiLLMCaller
from utils import extract_json_string, is_file_url, read_file, geocode
from crawl4ai import CrawlerMonitor, DisplayMode
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer
from crawl4ai.deep_crawling import BestFirstCrawlingStrategy
from parsing import post_process_web_parsing, post_process_result
from service.es_api import get_doc_from_es_by_id, write_provider_info_to_es, write_offering_info_to_es
import uuid

from crawl4ai import RateLimiter
import global_vars

SUMMARY = True
CACHE_DIR = "crawl_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def get_cache_key(url):
    return hashlib.md5(url.encode()).hexdigest()

def load_from_cache(url):
    cache_key = get_cache_key(url)
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.pkl")
    if os.path.exists(cache_file):
        with open(cache_file, "rb") as f:
            return pickle.load(f)
    return None

def save_to_cache(url, content):
    cache_key = get_cache_key(url)
    cache_file = os.path.join(CACHE_DIR, f"{cache_key}.pkl")
    with open(cache_file, "wb") as f:
        pickle.dump(content, f)
        
def simplify_result(result):
    try:
        # 确保 'mainOfferingAddress' 存在并且包含 'name'
        main_address = result.get('mainOfferingAddress', {}).get('name', 'null')
        simplify_result = f"Provider: {result.get('businessFullName', 'null')}"
        simplify_result += f"\nMain Address: {main_address}"
    
        # 确保 'offerings' 存在并且是一个列表
        offerings = result.get('offerings', [])
        for idx, offering in enumerate(offerings):
            offering_name = offering.get('offeringName', 'Unknown Offering')
            
            # 检查 schedule 是否为 None
            schedule = offering.get('schedule')
            if schedule is None:
                start_date = 'null'
                end_date = 'null'
                # ttime = 'null'
            else:
                start_date = schedule.get('startDate', 'null')
                end_date = schedule.get('endDate', 'null')
                # ttime = schedule.get('time', 'null')
            
            # 检查 schedule 是否为 None
            ageGroup = offering.get('ageGroup')
            if ageGroup is None:
                min_age = 'null'
                max_age = 'null'
            else:
                min_age = ageGroup.get('gte', 'null')
                max_age = ageGroup.get('lte', 'null')
                
            # 检查 location 是否为 None
            location = offering.get('location','null')
            if location is None:
                location_name = 'null'
            else:
                location_name = location.get('name', 'null')
            
            camp_session_options = offering.get('campSessionOptions', 'null')
            
            simplify_result += f'\n[{idx+1}] {offering_name}'
            simplify_result += f'\n  Date: {start_date} | {end_date}'
            # simplify_result += f'\n  Time: {ttime}'
            simplify_result += f'\n  age: {min_age}-{max_age}'
            simplify_result += f'\n  Location: {location_name}'
            simplify_result += f'\n  Camp Session: {camp_session_options}'
            simplify_result += '\n'
    except Exception as e:
        traceback.print_exc()
        
    return simplify_result
def parsing(context, prompt_path = '../prompt/web_prompt_summer.md', images=[]):
    prompt = read_file(prompt_path)
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
    
    for image in images:
        messages[-1]['content'].append({
            "type": "image_url",
            "image_url": {
                'url': image
            }
        })
        
    
    
    if not SUMMARY:
        parsing_result = None
        for parse_count in range(3):
            print(f"parse count {parse_count}")
            try:
                if parse_count <= 2:
                    # rsp = call_llm(messages, model_name='gemini-2.0-flash-001')
                    # rsp = GeminiLLMCaller().call(messages, model_name='gemini-2.0-flash-001')
                    # rsp = GeminiLLMCaller().call(messages, model_name='gemini-1.5-flash')
                    rsp = GeminiLLMCaller().call(messages, model_name='gemini-2.0-flash-lite')
                    # rsp = GeminiLLMCaller().call(messages, model_name='gemini-2.0-flash')
                    # rsp = call_llm(messages, source='openai', model_name='gpt-4o')
                else:
                    rsp = GeminiLLMCaller().call(messages, model_name='gemini-2.0-flash-thinking-exp')
                parsing_result = extract_json_string(rsp)
                parsing_result = json.loads(parsing_result)
                break
            except Exception as e:
                print(f"Error parsing, {str(e)}")
                parsing_result = None
        return parsing_result
            
    else:
        # rsp = GeminiLLMCaller().call(messages, model_name='gemini-2.0-flash')
        rsp = GeminiLLMCaller().call(messages, model_name='gemini-2.0-flash-lite')
        messages.append({
            "role": "assistant",
            "content": rsp
        })
        with open("output/origin.md", 'w') as f:
            f.write(rsp)
        
        with open('../prompt/summary.md') as f:
            summary_template = f.read()
        # summary
        messages.append({
            "role": "user",
            "content": summary_template
        })

        parsing_result = None
        for parse_count in range(3):
            print(f"parse count {parse_count}")
            try:
                if parse_count <= 2:
                    # rsp = GeminiLLMCaller().call(messages, model_name='gemini-2.0-flash')
                    rsp = GeminiLLMCaller().call(messages, model_name='gemini-2.0-flash-lite')
                else:
                    rsp = GeminiLLMCaller().call(messages, model_name='gemini-2.0-flash')
                parsing_result = extract_json_string(rsp)
                parsing_result = json.loads(parsing_result)
                break
            except Exception as e:
                print(f"Error parsing, {str(e)}")
                parsing_result = None
        return parsing_result
    
    
async def crawl_website_all_links(base_url, level=2):
    
    content_filter = PruningContentFilter(
        threshold=0.35,
        # "fixed" or "dynamic"
        # threshold_type="dynamic",
        # Ignore nodes with <5 words
        # min_word_threshold=3
    )
    md_generator = DefaultMarkdownGenerator(content_filter = content_filter)
    config = CrawlerRunConfig(
        markdown_generator=md_generator,
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
                if count == 1:
                    content = result.markdown.raw_markdown
                else:
                    content = result.markdown.fit_markdown
                # content = result.markdown
                if content in seen_contents:
                    print(f"Duplicate content found, skipping...")
                else:
                    if content.lower().find('summer') >= 0 or content.lower().find('camp') >= 0:
                        pages.append({
                            'content': content,
                            'url': result.url,
                            'title': result.metadata.get('title', '')
                        })
                        seen_contents.add(content)
                    else:
                        print(f"summer camp not found, skipping...")
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
        
def process(idx, base_url):
    print(idx, base_url)
    start_time = time.time()
    page_count = 0
    content = load_from_cache(base_url)
    # content = ''
    # if content:
    #     print(f"Loaded {base_url} from cache.")
    #     page_count = len(content.split('\n\n---\nPage '))
    # else:
    #     content, page_count = crawl_website_all_links(base_url, level=3)
    #     save_to_cache(base_url, content)

    with open('output/content.md', 'w') as f:
        f.write(content)
    end_time = time.time()

    if not content:
        # print(f"no summer camp content, skip")
        return None, page_count
    
    token_num = count_tokens(content)
    print(f"token_num={token_num}, {base_url} crawled in {end_time - start_time} seconds")
    
    parsing_result = parsing(content)
    if not parsing_result:
        return None, page_count
        
    # businessID = str(uuid.uuid4())
    businessID = '1'
    # businessFullName = "test"
    # parsing_result['businessFullName'] = businessFullName
    # parsing_result['businessID'] = businessID
    parsing_result['website'] = base_url
    
    post_process_web_parsing(parsing_result, businessID)
    # offerings = parsing_result['offerings']
    # del parsing_result['offerings']
    # provider = parsing_result
    
    # write_provider_info_to_es("provider_pre", provider)
    # write_offering_info_to_es("offering_pre" ,offerings)
    
    t = simplify_result(parsing_result)
    parsing_result = json.dumps(parsing_result, indent=4, ensure_ascii=False)

    with open(f"output/test/{idx}.json", "w") as f:
        f.write(parsing_result)
    with open(f"output/test/{idx}.txt", "w") as f:
        f.write(t)
        
    return t, page_count


def process_compare(idx, gt, result):
    prompt = read_file('../prompt/sumer_camp_extract.md')
    context = prompt.format(
        gt=gt,
        model_result=result
    )
    # print(context)
    messages = [
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
    rsp = GeminiLLMCaller().call(messages, 'gemini-2.0-flash-001')
    return rsp
    
async def single_test():
    base_urls = [
        # 'https://www.jefflakecamp.com/'
        # 'https://dingersnj.com/camp-registration'
        # 'https://www.funkyfunart.com/summer-camp-info'
        # 'https://www.gshnj.org/en/discover/our-council/our-camps/summer-camp.html'
        # 'http://www.njballet.org/'
        # 'http://www.artcenternj.org/'
        # 'http://www.ducretarts.org/'
        # 'https://centercourtacademy.com/',
        # 'https://www.classactpas.com/summer-rec-program'
        # 'https://missshona.com/'
        # 'https://iisummer.com/'
        # 'https://missshona.com/'
        # 'https://www.jefflakecamp.com/',
        # 'https://newarkmuseumart.org/learn/camp-nmoa-2024/'
        # 'https://jccmetrowest.org/programs/camps/basketball/'
        # 'https://www.littlefoxesplay.com/summer-camp'
        # 'http://www.ducretarts.org/'
        # 'https://jccmetrowest.org/programs/camps/basketball/'
        # 'https://campriverbend.com/'
        # 'https://www.montclairymca.org/camps/camp-at-the-lake/'
        # 'https://jazzhousekids.org/programs/montclair/jazz-house-summer-workshop/'
        # 'https://www.newhorizonsdaycamp.com/'
        # 'https://hartshornarboretum.org/program/camps-2/'
        # 'https://trilax.com/'
        # 'https://www.theartfarms.com/'
        # 'http://usatravelfun.com/'
        # 'http://www.njpac.org/',
        # 'https://www.daycampsunshine.org/'
        # 'https://www.lockedinlax.com/'
        # 'http://www.acrossthefloor.com/'
        'https://hartshornarboretum.org/program/camp/'
        # 'https://www.baldeaglelax.com/contact'
    ]
    results = []
    for idx, base_url in enumerate(base_urls):
        result = await process(idx, base_url)
        # result = await process_two_stage(idx, base_url)
        results.append(result)
        print(f"Processed {idx+1}/{len(base_urls)}")


def multi_test():
    data_path = 'data/parsing check - 15.csv'
    # data_path = 'data/parsing check - Sheet1 3-20.csv'
    data = pd.read_csv(data_path, nrows=None)
    parsing_result_column = 'Parsing 2025/3/22'
    
    if parsing_result_column not in data.columns:
        data[parsing_result_column] = ''
    # if 'page_count' not in data.columns:
    #     data['page_count'] = ''
    # if 'franchise' not in data.columns:
    #     data['franchise'] = ''
    # if 'activity' not in data.columns:
    #     data['activity'] = ''
    

    data = data.fillna('')

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(process, idx, row['Website']): idx for idx, row in data.iterrows()}
        for future in concurrent.futures.as_completed(futures):
            idx = futures[future]
            try:
                result, page_count = future.result()
                data.at[idx, parsing_result_column] = result
                data.to_csv(data_path, index=False)
            except Exception as e:
                print(f"An error occurred: {e}")
    
    data.to_csv(data_path, index=False)
    
def multi_test_compare():
    data_path = 'data/parsing check - Sheet1 3-20.csv'
    data = pd.read_csv(data_path, nrows=None)
    
    if 'AI compare v2' not in data.columns:
        data['AI compare v2'] = ''
    data = data.fillna('')

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(process_compare, idx, row['GroundTruth 3/19'], row['Parsing 2025/3/20 v2']): idx for idx, row in data.iterrows() if row['GroundTruth 3/19'] and row['Parsing 2025/3/20 v2']}
        for future in concurrent.futures.as_completed(futures):
            idx = futures[future]
            try:
                result = future.result()
                data.at[idx, 'AI compare v2'] = result
                data.to_csv(data_path, index=False)
            except Exception as e:
                print(f"An error occurred: {e}")
    
    data.to_csv(data_path, index=False)
def main():
    # await single_test()
    multi_test()
    # await multi_test_week3()
    # await multi_test_virtual_poster()
    # await multi_test_franchise()
    
if __name__ == "__main__":
    # asyncio.run(main())
    global_vars.init_globals()
    multi_test()
    # multi_test_compare()