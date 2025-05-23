import json
import pandas as pd
import hashlib
import sys
sys.path.append('../')
from service.es_api import get_doc_from_es_by_id, write_provider_info_to_es, write_offering_info_to_es
from utils import extract_json_string, is_file_url, read_file, geocode
from service.llm import call_llm
from parsing import post_process_web_parsing

def parsing(context, summary=False):
    prompt = read_file('../prompt/web_prompt_summer.md')
    # prompt = read_file('../prompt/web_prompt_offering.md')
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
    
    
    parsing_result = {}
    for parse_count in range(3):
        print(f"Parsing {base_url}, parse count {parse_count}")
        try:
            if parse_count == 0:
                rsp = call_llm(messages, model_name='gemini-2.0-flash-001')
            elif parse_count == 1:
                rsp = call_llm(messages, model_name='gemini-2.0-pro-exp-02-05')
            elif parse_count == 2:
                rsp = call_llm(messages, model_name='gemini-2.0-flash-thinking-exp-01-21')
            parsing_result = extract_json_string(rsp)
            parsing_result = json.loads(parsing_result)
            break
        except Exception as e:
            print(f"Error parsing {base_url}, {str(e)}")
            parsing_result = {}
    return parsing_result
    

data_path = 'data/Provider-Morris-Essex - Merge-2453.csv'
data = pd.read_csv(data_path)
data = data.fillna('')
if 'parsing result' not in data.columns:
    print('add parsing result')
    data['parsing result'] = ''
for idx, row in data.iterrows():
    if not row['GT planning']:
        continue
    # if row['parsing result']:
    #     continue
    
    
    base_url = row['Website']
    doc_id = hashlib.md5(base_url.encode()).hexdigest()
    print(idx, base_url)
    doc = get_doc_from_es_by_id("provider_test", doc_id)
    if not doc:
        continue
    pages = doc['pages']
    businessFullName = doc['businessFullName']
    businessID = doc['businessID']
    data.at[idx, 'new_page_count'] = len(pages)
    
    if row['parsing result']:
        data.at[idx, 'offering_count'] = len(json.loads(row['parsing result'])['offerings'])
    
    
    concat_content = ''
    page_count = 0
    for page in pages:
        if page['content'].lower().find('summer') < 0 and page['content'].lower().find('camp') < 0:
            continue
        # 添加显性分隔符
        separator = f"\n\n---\nPage {page_count}: {page['title']}: {page['url']}\n---\n\n"
        concat_content += separator + page['content']
        page_count += 1
    
    
    if not concat_content or page_count > 100:
        continue
    
    parsing_result = parsing(concat_content)
    parsing_result['businessFullName'] = businessFullName
    parsing_result['businessID'] = businessID
    parsing_result['website'] = base_url
    data.at[idx, 'parsing result'] = json.dumps(parsing_result, indent=4)
    # data.to_csv(data_path, index=False)

    
    post_process_web_parsing(parsing_result, businessFullName, businessID)
    offerings = parsing_result['offerings']
    del parsing_result['offerings']
    provider = parsing_result

    write_provider_info_to_es("provider_pre", provider)
    write_offering_info_to_es("offering_pre" ,offerings)
    
    break
        
    