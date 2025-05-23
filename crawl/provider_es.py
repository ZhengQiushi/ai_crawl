from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import traceback
sys.path.append("../")
from elasticsearch import Elasticsearch, helpers
import json
import pandas as pd
import requests
from service.llm import call_llm
from utils import extract_json_string

es = Elasticsearch(
    "https://5c9b418770044b7daf9800142664e127.westus2.azure.elastic-cloud.com:443",
    api_key="VDI2TVlKVUJxbWI1c3FaYTBybkw6Z3EwY2ZuTnFTeUtGejJscWJ6Nmc5dw=="
)

def get_pages_from_es(name, index_name, url):
    
    index_name = "provider_dev"
    query = {
            "query": {
                "match_phrase": {
                    "website": url
                }
            }
        }
    response = es.search(index=index_name, body=query)
    try:
        pages = response['hits']['hits'][0]['_source']['pages']
    except:
        pages = []
    return pages

def process_row(row):
    idx = row.name  # 获取行索引
    url = row['Website']
    name = row['Name']
    county = row['County'].lower()
    
    # if name != "Natalie's School Of Performing Arts":
    #     return
    print(idx, name)
    
    
    
    # row['robot'] = check_robot_exist(url)
    pages = get_pages_from_es(name, county, url)
    # print(pages[0])
    row['page_count'] = len(pages)
    
    concat_content = ''
    for page_id, page in enumerate(pages):
        separator = f"\n\n---\nPage {page_id}: {page['title']}: {page['url']}\n---\n\n"
        concat_content += separator + page['content']

    if not concat_content:
        return idx, row
    
    with open('../prompt/web_check.md', 'r') as f:
        prompt = f.read()
    
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
                    "text": concat_content
                }
            ]
        }
    ]
    try:
        rsp = call_llm(messages, source='gemini')
        rsp = extract_json_string(rsp)
        rsp = json.loads(rsp)
        
        # 将结果写入 row
        row['institution_size'] = rsp.get('institution_size', '')
        row['has_summer_camp'] = rsp.get('has_summer_camp', '')
        row['multi_location'] = rsp.get('multi_location', '')
        row['kids_enrichment_program'] = rsp.get('kids_enrichment_program', '')
        row['franchise'] = rsp.get('franchise', '')
        
        print(rsp)
    except Exception as e:
        print(json.dumps(rsp, indent=4))
        print(f'Error processing {name}: {str(e)}')
        traceback.print_exc()
    
    return idx, row



def main():
    

    data = pd.read_csv('data/Provider-Morris-Essex - Merge.csv', nrows=None)
    # data = data.drop(columns=['Unnamed: 17'])
    data = data.fillna('')
    
    
    # 添加新列时先初始化
    data['page_count'] = ''
    data['institution_size'] = ''
    data['has_summer_camp'] = ''
    data['multi_location'] = ''
    data['kids_enrichment_program'] = ''
    data['franchise'] = ''
    
    # 使用线程池并发处理
    with ThreadPoolExecutor(max_workers=4) as executor:
        # results = list(executor.map(process_row, [row for _, row in data.iterrows()]))
        # for idx, row in enumerate(data.iterrows()):
        #     _, original_row = row
        #     updated_row = process_row(original_row)
        #     data.loc[idx] = updated_row
        
        futures = {executor.submit(process_row, row): idx for idx, row in data.iterrows()}
        # 按原始顺序收集结果
        results = [None] * len(data)
        for future in as_completed(futures):
            idx, updated_row = future.result()
            results[idx] = updated_row
    
    data = pd.DataFrame(results)
    data.to_csv('data/Provider-merge-result.csv', index=False)
    

if __name__ == "__main__":
    main()