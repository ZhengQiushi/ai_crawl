import json
import concurrent.futures
import time
import traceback
from google import genai
from google.genai.types import Tool, GenerateContentConfig, GoogleSearch
from urllib.parse import unquote, urlparse
import requests
import pandas as pd

def extract_original_url(url):
    """Extract the final URL after following redirects."""
    try:
        response = requests.get(url, allow_redirects=True, timeout=10)
        return response.url
    except:
        return None
    
def normalize_domain(domain):
    if domain.startswith("www."):
        return domain[4:]
    return domain
def check_summer_camp_availability(idx, url):
    # url = data.loc[idx, 'Website']
    print(idx, url)
    try:
        api_key = 'AIzaSyDIrADHmfO8nT6rMKAOUTJVR6E-6f_QSRo'
        # api_key = 'AIzaSyCHv4CIWrj3pCjQAjuT8NlbQWxcSAVZcrI'
        # api_key = 'AIzaSyBxA5dsvx6P8kVYXC0iax0Xre5HQd3TcVY'
        api_key = 'AIzaSyCIi0xFKSMV1jwKF0n-reLFszGsIWFlEbo'
        client = genai.Client(api_key=api_key)
        
        
        # model_id = "gemini-2.0-pro-exp-02-05"
        model_id = "gemini-2.0-flash-001"

        google_search_tool = Tool(google_search=GoogleSearch())

        # Generate content based on the provided URL
        
        max_retries = 10000
        for attempt in range(1, max_retries + 1):
            try:
                response = client.models.generate_content(
                    model=model_id,
                    contents=f"""{url}

        Given the above link, determine if the organization offers a 2025 summer camp or summer program for kids.""",
                    config=GenerateContentConfig(
                        tools=[google_search_tool],
                        response_modalities=["TEXT"],
                        temperature=0,
                        top_p=0
                    )
                )
                break
            except Exception as e:
                print(f"Attempt {attempt} failed: {e}")
                time.sleep(3)
            

        # Extract the input URL's domain
        input_domain = normalize_domain(urlparse(url).netloc)

        # Check grounding chunks for matching domains
        grounding_chunks = response.candidates[0].grounding_metadata.grounding_chunks
        if not grounding_chunks:
            return False, None
        hit_list = []
        print(idx, len(grounding_chunks))
        for chunk in grounding_chunks:
            original_url = extract_original_url(chunk.web.uri)
            if not original_url:
                continue
            chunk_domain = urlparse(original_url).netloc
            chunk_domain = normalize_domain(chunk_domain)
            if chunk_domain == input_domain:
                hit_list.append(original_url)

        # If no matching domain is found
        print(idx, hit_list)
        if len(hit_list) > 0:
            return True, hit_list
        return False, None

    except Exception as e:
        traceback.print_exc()
        return False, None

if __name__ == "__main__":
    
    data_path = "data/Provider-Morris-Essex - Merge-2453.csv"
    data = pd.read_csv(data_path, nrows=None)
    
    # data['summer_link'] = ''
    # data['done'] = False
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(check_summer_camp_availability, idx, row['Website']): idx for idx, row in data.iterrows() if row['done'] == True}
            for future in concurrent.futures.as_completed(futures):
                idx = futures[future]
                try:
                    result, hit_list = future.result()
                    if hit_list:
                        data.at[idx, 'summer_link'] = '\n'.join(hit_list)
                    data.at[idx, 'done'] = True
                    # data.to_csv(data_path, index=False)
                except Exception as e:
                    print(f"An error occurred: {e}")
        
    # data.to_csv(data_path, index=False)