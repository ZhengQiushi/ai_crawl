import asyncio
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

def extract_domain(url):
    try:
        # 添加http://前缀如果不存在（urlparse需要）
        if not url.startswith(('http://', 'https://')):
            url = 'http://' + url
        parsed = urlparse(url)
        domain = parsed.netloc
        # 移除www.前缀如果存在
        domain = domain.replace('www.', '')
        return domain
    except:
        return None

def load_data_from_excel(file_path, num_rows):
    """
    从 Excel 文件中读取数据并进行处理。

    :param file_path: Excel 文件路径
    :param num_rows: 读取的行数
    :return: 处理后的数据列表
    """
    try:
        df = pd.read_excel(file_path, nrows=num_rows)  # 限制读取行数
    except FileNotFoundError:
        global_vars.logger.error(f"File not found: {file_path}")
        return []
    except Exception as e:
        global_vars.logger.error(f"Error reading Excel file: {e}")
        return []

    # 确保 businessID 是字符串类型
    try:
        df['businessID'] = df['businessID'].astype(str)
    except KeyError:
        global_vars.logger.error("Column 'businessID' not found in Excel file.")
        return []

    # 处理空值
    df = df.fillna('')
    df['domain'] = df['website'].apply(extract_domain)
    real_provider = [
        '9427',
        '9460',
        '100000',
        '100002',
        '100003',
        '100004',
        '100005',
        '100006',
        '100007',
        '100008',
        '100009',
        '100010',
        '100011',
        '100012',
        '100013',
        '100014',
        '100015',
        '100016',
        '100017',
        '201093',
        '202957',
        '205286'
        ]
    df_filtered = df[df['businessID'] == "223251"] # df[~df['businessID'].isin(real_provider)]

    data = df_filtered.to_dict('records')  # 转换为字典列表

    # Add job_id field to each record
    for item in data:
        item['job_id'] = item['businessID']  # Ensure businessID is a string
    global_vars.logger.error(f"读excel长度 {len(data)}")
    return data


    

async def main():
    parser = argparse.ArgumentParser(description='Run the server.')
    parser.add_argument('--County', required=True, type=str, help='Comma-separated list of Counties')
    parser.add_argument('--State', required=True, type=str, help='State')
    parser.add_argument('--Num', required=True, type=str, help='Number of items to process')
    parser.add_argument('--Config', required=True, type=str, help='Configuration file path')
    args = parser.parse_args()

    global_vars.init_globals(args.Config)

    logger = logging.getLogger('Main')
    counties = [c.strip() for c in args.County.split(',')]

    # Load data from Excel file
    excel_file_path = '/home/azureuser/ai_crawl/data/provider_data_combined_250527.xlsx'
    combined_data = load_data_from_excel(excel_file_path, int(args.Num))


    global_vars.logger.error(f"开始爬虫任务, 共 {len(combined_data)} 个条目")
    crawler = Crawler(
        max_processes=1,
        max_concurrent_per_thread=1,
        max_depth=2,
        timeout=30,
        batch_size=1,
        max_retries=3,
        max_pages_per_website=500
    )
    combined_data = combined_data[0:500]
    crawler.crawl_website(combined_data)


if __name__ == "__main__":
    asyncio.run(main())