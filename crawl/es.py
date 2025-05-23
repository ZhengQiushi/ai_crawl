from concurrent.futures import ThreadPoolExecutor
import sys
import traceback
sys.path.append("../")
from elasticsearch import Elasticsearch, helpers
import json
import pandas as pd
import requests
from service.llm import call_llm
from utils import extract_json_string, geocode
import uuid

es = Elasticsearch(
    "https://5c9b418770044b7daf9800142664e127.westus2.azure.elastic-cloud.com:443",
    api_key="VDI2TVlKVUJxbWI1c3FaYTBybkw6Z3EwY2ZuTnFTeUtGejJscWJ6Nmc5dw=="
)

mapping = {
    "mappings": {
        "properties": {
            "offeringID": { "type": "keyword" },
            "businessID": { "type": "keyword" },
            "businessFullName": { "type": "text", "copy_to": "businessFullNameEmbeddings"},
            "businessFullNameEmbeddings": { "type": "semantic_text", "inference_id": "google_ai_studio_completion"},
            "offeringName": { "type": "text", "copy_to": "offeringNameEmbeddings"},
            "offeringNameEmbeddings": { "type": "semantic_text", "inference_id": "google_ai_studio_completion"},
            "schedule": {
                "type": "nested",
                "properties": {
                    "startDate": { "type": "date" },
                    "endDate": { "type": "date" },
                    "durationSeason": { "type": "keyword" },
                    "classDay": { "type": "keyword" },
                    "time": { "type": "keyword" },
                    "frequency": { "type": "keyword" },
                    "blackoutDate": { "type": "keyword" }
                }
            },
            "pricing": { "type": "text" },
            "ageGroup": { "type": "integer_range" },
            "skillLevel": { "type": "keyword" },
            "location": {
                "properties": {
                    "name": { "type": "text" },
                    "geo_info": { "type": "geo_point" },
                    "zipcode": { "type": "keyword" },
                }
            },
            "locationDisplayName": { "type": "keyword" },
            "locationType": { "type": "keyword" },
            "RSVP": { "type": "text" },
            "RSVPDeadline": { "type": "date" },
            "hyperlink": { "type": "text" },
            "activityCategory": { "type": "text", "copy_to": "activityCategoryEmbeddings"},
            "activityCategoryEmbeddings": { "type": "semantic_text", "inference_id": "google_ai_studio_completion"},
            "activity": { "type": "text", "copy_to": "activityEmbeddings"},
            "activityEmbeddings": { "type": "semantic_text", "inference_id": "google_ai_studio_completion"},
            "offeringType": { "type": "keyword"},
            "facility": { "type": "keyword" },
            "transportation": { "type": "keyword" },
            "campSessionOptions": { "type": "keyword" },
            "earlyDropOff": { "type": "keyword" },
            "latePickup": { "type": "keyword" },
            "lunchIncluded": { "type": "keyword" },
            "sourceLink": { "type": "keyword" },
            "offeringInsightSummary": { "type": "text", "copy_to": "offeringInsightSummaryEmbeddings"},
            "offeringInsightSummaryEmbeddings": { "type": "semantic_text", "inference_id": "google_ai_studio_completion" },
        }
    }
}

# index_name = "offering_dev"
# if es.indices.exists(index=index_name):
#     es.indices.delete(index=index_name)
# es.indices.create(index=index_name, body=mapping)

def check_robot_exist(url):
    try:
        # 构造robots.txt的URL
        if not url.startswith(('http://', 'https://')):
            url = 'http://' + url
        
        robots_url = url.rstrip('/') + '/robots.txt'
        # print(robots_url)
        # 发送HTTP请求
        response = requests.get(robots_url, timeout=15)
        
        # 检查响应状态码
        if response.status_code == 200:
            content = response.text
            # print(content)
            # 检查是否包含有效的robots.txt指令
            if any(line.startswith(('User-agent:', 'Disallow:', 'Allow:', 'Sitemap:')) 
                   for line in content.splitlines()):
                return True
            return False
        else:
            return False
    except Exception as e:
        # 如果发生任何异常，返回False
        print(f"Error checking robots.txt for {url}: {str(e)}")
        return False

def get_pages_from_es(name):
    
    index_name = "web_content"
    query = {
            "query": {
                "match_phrase": {
                    "businessFullName": name
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
    
    # if url != "The World of Knowledge":
    #     return
    print(idx, name)
    
    
    
    # row['robot'] = check_robot_exist(url)
    pages = get_pages_from_es(name)
    row['page_count'] = len(pages)
    
    concat_content = ''
    for page_id, page in enumerate(pages):
        separator = f"\n\n---\nPage {page_id}: {page['title']}: {page['url']}\n---\n\n"
        concat_content += separator + page['content']
    
    if not concat_content:
        return row
    
    with open('../prompt/web_prompt_summer.md', 'r') as f:
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
        businessFullName = rsp['businessFullName']
        if 'offerings' in rsp:
            for offering in rsp['offerings']:
                offering['businessFullName'] = businessFullName
                offering['offeringID'] = str(uuid.uuid4())
                if 'location' in offering and offering['location']:
                    geo_info = geocode(offering['location'])
                    offering['location'] = {
                        'name': offering['location'],
                        'geo_info': {
                            'lat': float(geo_info[0]),
                            'lon': float(geo_info[1])
                        } if geo_info else None,
                        'zipcode': geo_info[2] if geo_info else None
                    }
                if 'ageGroup' in offering:
                    offering['ageGroup'] = {
                        "gte": 0,
                        "lte": 100,
                    }
                    if 'minAge' in 'ageGroup' and offering['ageGroup']['minAge']:
                        offering['ageGroup']['gte'] = int(offering['ageGroup']['minAge'])
                    if 'maxAge' in 'ageGroup' and offering['ageGroup']['maxAge']:
                        offering['ageGroup']['lte'] = int(offering['ageGroup']['maxAge'])    
                        
                if 'offeringInsightSummary' in offering:
                    offering['offeringInsightSummary'] = "\n".join(offering['offeringInsightSummary'])
                if 'hyperlink' in offering and isinstance(offering['hyperlink'], list):
                    offering['hyperlink'] = " | ".join(offering['hyperlink'])
                if 'pricing' in offering and isinstance(offering['pricing'], list):
                    offering['pricing'] = " | ".join(offering['pricing'])
                if 'RSVPDeadline' in offering and offering['RSVPDeadline'] is None:
                    del offering['RSVPDeadline']
                
                # print(json.dumps(offering, indent=4))
                # es.index(index=index_name, id=offering['offeringID'], body=offering)
    except Exception as e:
        print(json.dumps(rsp, indent=4))
        print(f'Error processing {name}: {str(e)}')
        traceback.print_exc()
    
    return row



def main():
    

    data = pd.read_csv('data/Essex.csv', nrows=8)
    data = data.drop(columns=['Unnamed: 17'])
    data = data.fillna('')
    
    
    # 添加新列时先初始化
    data['page_count'] = ''
    data['robot'] = ''
    data['institution_size'] = ''
    data['has_summer_camp'] = ''
    
    print(data)
    
    # 使用线程池并发处理
    with ThreadPoolExecutor(max_workers=1) as executor:
        results = list(executor.map(process_row, [row for _, row in data.iterrows()]))
        
        
    # data.to_csv('data/Essex_result_test.csv', index=False)
    

if __name__ == "__main__":
    main()