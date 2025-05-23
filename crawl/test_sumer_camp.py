import sys, os
import time
import pandas as pd
import asyncio
import concurrent.futures
from crawl4ai import AsyncWebCrawler, CacheMode, CrawlerRunConfig
from crawl4ai.content_filter_strategy import PruningContentFilter, BM25ContentFilter
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '/home/azureuser/Trustbeez')))

from service.llm import call_llm
from utils import extract_json_string, is_file_url, read_file
from crawl4ai import RateLimiter
from tenacity import retry, stop_after_attempt, wait_exponential

OUTPUT_PATH = "/home/azureuser/Trustbeez/crawl/output/"
SUMMARY = False

def parsing(context):
    prompt = read_file('../prompt/web_prompt_summer.md')
    # print(prompt)
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
    rsp = call_llm(messages)
    return rsp

async def crawl_website_all_links(base_url, level=2):
    content_filter = PruningContentFilter()
    md_generator = DefaultMarkdownGenerator(content_filter=content_filter)
    config = CrawlerRunConfig(
        markdown_generator=md_generator,
        # cache_mode=CacheMode.ENABLED  # 启用缓存
    )
    dispatcher = MemoryAdaptiveDispatcher(
            check_interval=0,
            rate_limiter=RateLimiter(
                base_delay=(0, 0),
                max_delay=0
            ),
            max_session_permit=128
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
            results = await crawler.arun_many(urls=list(urls_to_visit), config=config, dispatcher=dispatcher)
            for url in urls_to_visit:
                visited_urls.add(url)

            urls_to_visit.clear()
            for result in results:
                if not result or not result.markdown:
                    continue
                internal_links = result.links.get("internal", [])
                content = result.markdown.fit_markdown
                print(result.url)
                # print(content)
                # print(result.markdown)
                if content not in seen_contents:
                    if content.lower().find("summer") == -1 and content.lower().find("camp") == -1:
                        pass
                    else:
                        pages.append({'content': content, 'url': result.url, 'title': result.metadata.get('title', '')})
                    seen_contents.add(content)
                    for link in internal_links:
                        if len(urls_to_visit) > 50:
                            break
                        if link['href'] not in visited_urls and link['text'] and not is_file_url(link['href']):
                            urls_to_visit.add(link['href'])
    concat_content = ''
    for idx, page in enumerate(pages):
        separator = f"\n\n---\nPage {idx}: {page['title']}: {page['url']}\n---\n\n"
        concat_content += separator + page['content']
    return concat_content

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
async def process(idx, base_url):
    start_time = time.time()
    content = await crawl_website_all_links(base_url, level=3)
    with open("output/content.md", "w") as f:
        f.write(content)
    end_time = time.time()
    print(f"{base_url} crawled in {end_time - start_time} seconds")
    
    print(base_url)
    if os.path.exists(md_path):
        print(f"Skipping {base_url}, output already exists.")
        return (idx, base_url, "Skipped")
    
    start_time = time.time()

    content = await crawl_website_all_links(base_url, level=3)
    
    with open(md_path, "w") as f:
        f.write(content)
    
    if content.find("summer") == -1 and content.find("camp") == -1:
        parsing_result = ""
    else:
        parsing_result = parsing(content)
        if parsing_result is not None:
            parsing_result = parsing_result.replace("```python", " ")
            parsing_result = parsing_result.replace("```", " ")
            parsing_result = parsing_result.replace("\"", " ")
    with open(csv_path, "w") as f:
        f.write(f"{idx},{base_url},\"{parsing_result}\"\n")
    
    print(f"{base_url} crawled in {time.time() - start_time} seconds")
    return (idx, base_url, parsing_result)

async def main():
    base_urls = [
        'http://www.njballet.org/',
        # "https://kingchessacademy.com/",
        # "https://rahwaybestafterschool.com/",
        # "https://paintfunstudio.com/",
        # "http://www.foundationforlearning.com/",
        # "http://www.confidancenj.com/",
        # "https://xcelswim.com/",
        # "https://jumpinenrichment.com/",
        # "http://www.wizardsofthemind.com/",
        # "http://www.pinspiration.com/",
        # "https://greatkidsplace.com/",
        # "https://www.sofive.com/",
        # "http://www.jayseldinphotos.com/",
        # "http://www.newlifechildcare.com/",
        # "http://www.nafusgolf.com/",
        # "http://www.quailbrookgolf.com/",
        # "http://www.morriscountyfarms.com/",
        # "https://www.essexcountyparks.org/",
    ]
    # results = []
    # with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
    #     future_to_url = {executor.submit(process, idx, url): url for idx, url in enumerate(base_urls)}
    #     for future in concurrent.futures.as_completed(future_to_url):
    #         url = future_to_url[future]
    #         try:
    #             data = future.result()
    #             results.append(data)
    #         except Exception as exc:
    #             print(f'{url} generated an exception: {exc}')

    # 从 CSV 文件中读取 url_head 列
    df = pd.read_csv('/home/azureuser/Trustbeez/summary-733.csv')  # 替换为你的 CSV 文件路径
    base_urls = df['url_head'].tolist()

    # 过滤掉包含 .gov 的 URL
    base_urls = [url for url in base_urls if '.gov' not in url]

    for idx, base_url in enumerate(base_urls):
        try:
            result = await process(idx, base_url)
        except Exception as e:
            print(f"Error processing {base_url}: {e}")
            continue



    # 重新从 output 目录读取所有 CSV 进行合并
    csv_files = [f for f in os.listdir(OUTPUT_PATH) if f.endswith(".csv") and f != "merged_results.csv"]
    all_data = []

    for csv_file in csv_files:
        file_path = os.path.join(OUTPUT_PATH, csv_file)
        df = pd.read_csv(file_path, header=None, names=['Index', 'Website', 'Parsing Result'])
        all_data.append(df)

    if all_data:
        merged_df = pd.concat(all_data, ignore_index=True)
        merged_df.sort_values(by=['Index'], inplace=True)
        merged_df.to_csv(os.path.join(OUTPUT_PATH, "merged_results.csv"), index=False)
        print("All results have been saved and merged.")
    else:
        print("No new data found to merge.")

if __name__ == "__main__":
    asyncio.run(main())