import sys
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

mapping = {
    "mappings": {
        "properties": {
            "businessFullName": { "type": "text", "inference_id": "my-inference-endpoint"},
            "offeringName": { "type": "text", "inference_id": "my-inference-endpoint"},
            "schedule": {
                "properties": {
                "startDate": { "type": "date" },
                "endDate": { "type": "date" },
                "durationSeason": { "type": "keyword" },
                "classDay": { "type": "keyword" },
                "time": { "type": "keyword" },
                "frequency": { "type": "keyword" },
                "blackoutDate": { "type": "date" }
                }
            },
            "pricing": { "type": "nested" },
            "ageGroup": { "type": "keyword" },
            "skillLevel": { "type": "keyword" },
            "location": {
                "properties": {
                    "name": { "type": "text" },
                    "geo_info": { "type": "geo_point" }
                }
            },
            "locationDisplayName": { "type": "keyword" },
            "locationType": { "type": "keyword" },
            "RSVP": { "type": "text" },
            "RSVPDeadline": { "type": "date" },
            "hyperlink": { "type": "nested" },
            "activityCategory": { "type": "text", "inference_id": "my-inference-endpoint"},
            "activity": { "type": "text", "inference_id": "my-inference-endpoint"},
            "offeringType": { "type": "keyword"},
            "facility": { "type": "keyword" },
            "transportation": { "type": "keyword" },
            "campSessionOptions": { "type": "keyword" },
            "campAmenities": { "type": "keyword" },
            "lunchIncluded": { "type": "keyword" },
            "sourceLink": { "type": "keyword" },
            "offeringInsightSummary": { "type": "text" },
        }
    }
}

index_name = "offerings"
es.indices.delete(index=index_name)

es.indices.create(index=index_name, body=mapping)

def geocode(address):
    api_key = 'AIzaSyDMAMDf9SmH31pM-Ik30yUykqu5M-qAPS4'
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": address, "key": api_key}
    response = requests.get(url, params=params).json()
    if response["status"] == "OK":
        location = response["results"][0]["geometry"]["location"]
        return (location["lat"], location["lng"])
    else:
        return None

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
    
def main():
    

    data = pd.read_csv('data/Essex.csv', nrows=None)
    data = data.drop(columns=['Unnamed: 17'])
    data = data.fillna('')
    
    
    # 添加新列时先初始化
    data['page_count'] = ''
    data['robot'] = ''
    data['institution_size'] = ''
    data['has_summer_camp'] = ''
    
    print(data)
    for idx, row in data.iterrows():
        url = row['Website']
        name = row['Name']
        # if url != 'https://www.pioneeracademy.org/':
        #     continue
        print(idx, name)
        
        data.at[idx, 'robot'] = check_robot_exist(url)
        # break
        pages = get_pages_from_es(name)
        data.at[idx, 'page_count'] = len(pages)
        
        concat_content = ''
        for page_id, page in enumerate(pages):
            # 添加显性分隔符
            separator = f"\n\n---\nPage {page_id}: {page['title']}: {page['url']}\n---\n\n"
            concat_content += separator + page['content']
        
        if not concat_content:
            continue
        
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
        rsp = call_llm(messages, source='gemini')
        rsp = extract_json_string(rsp)
        try:
            rsp = json.loads(rsp)
            print(json.dumps(rsp, indent=4))
            
            for offering in rsp['offerings']:
                geo_info = geocode(offering['location'])
                
                es.index(index=index_name, body=offering)
            # if 'institution_size' in rsp:
            #     data.at[idx, 'institution_size'] = rsp['institution_size']
            # if 'has_summer_camp' in rsp:
            #     data.at[idx, 'has_summer_camp'] = rsp['has_summer_camp']
            # if 'multi_location' in rsp:
            #     data.at[idx, 'multi_location'] = rsp['multi_location']
            # if 'training_program' in rsp:
            #     data.at[idx, 'training_program'] = rsp['training_program']
        except:
            print(f'json error, {name}')
        break
        
        
    data.to_csv('data/Essex_result_test.csv', index=False)
    

if __name__ == "__main__":
    main()