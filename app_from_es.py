import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from playwright.async_api import async_playwright
from urllib.parse import urlparse, urljoin
from typing import List, Set, Dict, Optional
import logging
import threading
import time
from bs4 import BeautifulSoup
import asyncio
import logging
import requests
from link_extractor import LinkExtractor
from url_type_checker import *
from crawler import *
from enum import Enum

import csv
import pandas as pd
from elasticsearch import Elasticsearch
import global_vars
import argparse
import logging, multiprocessing

def connect_to_es():
    es_config = {
        "hosts": global_vars.config.get("ES_HOST"),
        "api_key": global_vars.config.get("ES_API_KEY")
    }
    return Elasticsearch(**es_config)

def get_es_data(es, index_name, state, counties):
    """
    从 Elasticsearch 中使用 scroll API 检索所有数据，支持多个 county。

    :param es: Elasticsearch 客户端
    :param index_name: 索引名称
    :param state: 州
    :param counties: county 列表
    :return: 检索到的所有数据列表
    """

    county_matches = [{"match": {"county": county}} for county in counties]
    query = {
        "query": {
            # "term": {
            #     "domain": "gardenstatetennis.com"
            # }
            "terms": {
                "businessID": [240658, 202957] # 233740, 202957, 
            }
            # "bool": {
            #     "must": [
            #         {"bool": {"should": county_matches}}  # 使用 should 来匹配任何一个 county
            #     ]
            # }

            # "bool": {
            #     "must": [
            #         {
            #             "exists": {
            #                 "field": "summerCampPages"
            #             }
            #         }
            #     ]
            # }
            # "match_all": {

            # }
            # "bool": {
            #     "must_not": [
            #         {
            #             "exists": {
            #                 "field": "pages"
            #             }
            #         }
            #     ]
            # }
        },
        "_source": ["businessID", "businessFullName", "county", "state", "website", "googleReview", "googleReviewRating", "googleReviewCount", "domain", "googleEntry"],  # 返回需要的字段，加上review相关字段
        "script_fields": {
            "pages_length": {
                "script": {
                    "source": """
                        if (params._source.containsKey('pages') && params._source.pages != null && params._source.pages instanceof List) {
                            return params._source.pages.size();
                        } else {
                            return 0;
                        }
                    """
                }
            }
        },
    }
    
    scroll_size = 2000  # 每次 scroll 的大小，根据集群情况调整
    results = []

    # 初始化 scroll
    response = es.search(index=index_name, body=query, scroll='1m', size=scroll_size, request_timeout=360)
    scroll_id = response['_scroll_id']
    hits = response['hits']['hits']

    while len(hits) > 0:
        # 提取所需数据
        for hit in hits:
            source = hit["_source"]
            result = {
                "businessID": str(source["businessID"]),  # Ensure businessID is a string
                "businessFullName": source["businessFullName"],
                "county": source.get("county", ""),
                "state": source.get("state",""),
                "website": source["website"],
                "googleReview": source.get("googleReview", ""),  # 处理可能缺失的字段
                "googleReviewRating": source.get("googleReviewRating", ""),
                "googleReviewCount": source.get("googleReviewCount", ""),
                "domain": source.get("domain", ""),
                "googleEntry": source.get("googleEntry", ""),
                "pages_length": hit["fields"]["pages_length"][0],  # 获取 pages 的长度
                'job_id': str(source["businessID"]),  # 添加job_id
            }
            results.append(result)
        global_vars.logger.error(f"长度 {len(results)}")
        try:
            response = es.scroll(scroll_id=scroll_id, scroll='1m', request_timeout=360)
        except Exception as e:
            print(f"Error during scroll: {e}")
            break

        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']

    # 清除 scroll 上下文
    try:
        es.clear_scroll(scroll_id=scroll_id)
    except Exception as e:
        print(f"Error clearing scroll: {e}")

    global_vars.logger.error(f"final 长度 {len(results)}")
    return results

def compare_data(es_data):
    """
    筛选出 pages 长度小于等于 3 的数据。
    :param es_data: 从 ES 检索到的数据列表。
    :return: pages长度小于等于3的数据列表.
    """
    empty_pages_in_es = []

    for hit in es_data:
        if hit.get('pages_length', 0) <= 3:
            empty_pages_in_es.append(hit)
    
    return empty_pages_in_es

def write_to_csv(data, file_name):
    """
    将数据写入 CSV 文件。
    :param data: 要写入的数据列表（字典列表）
    :param file_name: CSV 文件名
    """
    if not data:
        print(f"No data to write to {file_name}")
        return

    fieldnames = data[0].keys()  # 获取字典的键作为列名
    with open(file_name, mode='w', encoding='utf-8', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def retry(state, counties, nums):
    """
    重试逻辑：从 ES 获取数据，筛选 pages 长度，返回指定数量的数据。

    :param state: 州
    :param counties: county 列表
    :param nums: 返回数据的数量
    :return: 筛选后的数据列表
    """
    es = connect_to_es()
    index_name = global_vars.config.get("ES_INDEX_NAME")

    es_data = get_es_data(es, index_name, state, counties)

    empty_pages_in_es = compare_data(es_data)

    #write_to_csv(empty_pages_in_es, 'data/empty_pages_in_es.csv') # 注释掉，不再写文件

    return empty_pages_in_es[:nums]

def first_retry(state, counties, nums):
    """
    重试逻辑：从 ES 获取数据，筛选 pages 长度，返回指定数量的数据。

    :param state: 州
    :param counties: county 列表
    :param nums: 返回数据的数量
    :return: 筛选后的数据列表
    """
    es = connect_to_es()
    index_name = global_vars.config.get("ES_INDEX_NAME")

    es_data = get_es_data(es, index_name, state, counties)

    return es_data[:nums]


async def main():
    parser = argparse.ArgumentParser(description='Run the server.')
    parser.add_argument('--County', required=True, type=str, help='Comma-separated list of Counties')
    parser.add_argument('--State', required=True, type=str, help='State')
    parser.add_argument('--Num', required=True, type=str, help='Number of items to retry')
    parser.add_argument('--Config', required=True, type=str, help='Configuration file path')
    args = parser.parse_args()

    global_vars.init_globals(args.Config)

    logger = logging.getLogger('Main')
    counties = [c.strip() for c in args.County.split(',')]
    for i in range(1):  # 可以根据需要增加重试次数
        if i == 0:
            combined_data = first_retry(args.State, counties, int(args.Num))
        else:
            combined_data = retry(args.State, counties, int(args.Num))

        global_vars.logger.error(f"开始重试, 第 {i} 次，重试长度 {len(combined_data)}")
        crawler = Crawler(
            max_processes=4,
            max_concurrent_per_thread=8,
            max_depth=2,
            timeout=10,
            batch_size=5,
            max_retries=3,
            max_pages_per_website=500
        )
        crawler.crawl_website(combined_data)


if __name__ == "__main__":
    asyncio.run(main())