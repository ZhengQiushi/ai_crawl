import logging
import threading
import time
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from crawl4ai import PruningContentFilter, DefaultMarkdownGenerator
import os
import json
import hashlib
import sqlite3
from elasticsearch import Elasticsearch, helpers
# from threading import Thread
# from queue import Queue
from multiprocessing import Process, Queue
import queue
import multiprocessing
import os

class ESWriterProcess(Process):
    def __init__(self, queue, es_config, index_name):
        super().__init__()
        self.queue = queue
        self.es_config = es_config
        self.index_name = index_name
        self.batch_size = 3  # 调整批量写入大小
        self.shutdown_flag = multiprocessing.Value('b', False)  # 使用共享内存的布尔值
        
        # 配置日志记录到文件
        self.logger = logging.getLogger('ESWriterProcess')
        self.logger.setLevel(logging.WARNING)
        file_handler = logging.FileHandler('logs/es_writer_errors.log', mode='a')  # 追加模式
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        self._buffer = []
        self.md_generator = DefaultMarkdownGenerator(content_filter=PruningContentFilter())
        # 每个进程独立创建ES连接
        self.es = Elasticsearch(**self.es_config)
        logging.getLogger('elastic_transport.transport').setLevel(logging.WARNING)
        try:
            self.es.indices.delete(index=self.index_name)
        except:
            pass
        
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, body={
                "mappings": {
                    "properties": {
                        "businessID": {
                            "type": "keyword"
                        },
                        "mainOfferingAddress": {
                            "properties": {
                                "location": {
                                    "type": "geo_point"  # 显式定义为 geo_point 类型
                                }
                            }
                        }
                    }
                }
            })

    def run(self):

        while not self.shutdown_flag.value or not self.queue.empty():
            try:
                if self.shutdown_flag.value:
                    print(self.queue.qsize(), len(self._buffer))
                # 从队列获取数据，最多等待1秒
                item = self.queue.get(timeout=1)
                
                item['content'] = self.md_generator.generate_markdown(item['content'], item['url']).fit_markdown
                
                doc_id = hashlib.md5(item['base_url'].encode()).hexdigest()                
                content_hash = hashlib.md5(item['content'].encode()).hexdigest()
                es_doc = {
                    'doc_id': doc_id,
                    'script': {
                        "source": """
                            if (ctx._source.pages == null) {
                                ctx._source.pages = new ArrayList();
                            }
                            boolean pageExists = false;
                            for (page in ctx._source.pages) {
                                if (page.url == params.url) {
                                    pageExists = true;
                                    if (page.content_hash != params.content_hash) {
                                        page.title = params.title;
                                        page.content = params.content;
                                        page.content_hash = params.content_hash;
                                        page.last_modified = params.current_time;
                                    }
                                    break;
                                }
                            }
                            if (!pageExists) {
                                ctx._source.pages.add([
                                    'url': params.url,
                                    'title': params.title,
                                    'content': params.content,
                                    'content_hash': params.content_hash,
                                    'last_modified': params.current_time
                                ]);
                            }
                        """,
                        "lang": "painless",
                        "params": {
                            'url': item['url'],
                            'title': item['title'],
                            'content': item['content'],
                            'content_hash': content_hash,
                            'current_time': int(time.time() * 1000),
                        }
                    },
                    'upsert': {
                        'businessID': doc_id,
                        'pages': [{
                            'url': item['url'],
                            'title': item['title'],
                            'content': item['content'],
                            'content_hash': content_hash,
                            'last_modified': int(time.time() * 1000)
                        }],
                        'businessFullName': item['name'],
                        # 'franchise': item['row_data']['franchise'],
                        # 'locationType': item['row_data']['Offering Type'],
                        'contactPhone': item['row_data']['Phone'],
                        'website': item['base_url'],
                        'mainOfferingAddress': {
                            'name': item['row_data']['Address'],
                            'location': {
                                'lat': float(item['row_data']['Latitude']),
                                'lon': float(item['row_data']['Longitude'])
                            }
                        },
                        'interest': [
                            f"{item['row_data']['Category']}: {item['row_data']['Activity']}"
                        ],
                        'googleReview': item['row_data']['Reviews'],
                        'googleReviewRating': item['row_data']['Rating'],
                        'googleReviewCount': item['row_data']['Rating Count'],
                        # 'contactEmail': ,
                        # 'additionalOfferingAddress': ,
                        # 'socialMedia': ,
                        # 'aboutUs': ,
                    }
                }
                
                self._buffer.append(es_doc)
                # 当缓冲达到批量大小时立即写入
                if len(self._buffer) >= self.batch_size:
                    self._bulk_write()
                del item
            except queue.Empty:
                pass

        # 处理剩余数据
        if self._buffer:
            self._bulk_write()

    def _bulk_write(self):
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                actions = []
                for doc in self._buffer:
                    actions.append({
                        "_op_type": "update",
                        "_index": self.index_name,
                        "_id": doc['doc_id'],
                        "script": doc['script'],
                        "upsert": doc['upsert']
                    })
                
                # 增加超时时间
                success, failed = helpers.bulk(
                    self.es, 
                    actions, 
                    stats_only=True,
                    request_timeout=60  # 增加超时时间到60秒
                )
                # print(f"批量提交成功 {success} 条，失败 {failed} 条")
                self._buffer.clear()
                break
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    self.logger.warning(f"批量写入ES失败，正在重试 ({retry_count}/{max_retries}): {str(e)}")
                else:
                    self.logger.error(f"批量写入ES失败，已达到最大重试次数: {str(e)}")
                    self._buffer = []  # 清空缓冲区，避免数据堆积

    def shutdown(self):
        self.shutdown_flag.value = True


class WebsitePipeline:
    def __init__(self):
        # self.results = {}
        self.process_count = 0
        self.total_count = 0
        self.max_queue_size = 10000
        self.process_num = os.cpu_count() * 2 - 2
        print(f"process_num={self.process_num}")

    def open_spider(self, spider):
        self.total_count = len(spider.start_urls)
        if hasattr(spider, 'df'):
            self.df = spider.df
            self.csv_file = spider.csv_file
        
        # 多进程通信队列
        self.es_queue = Queue(maxsize=self.max_queue_size)
        self.es_writer = None
        # ES配置参数
        self.es_config = {
            "hosts": "https://5c9b418770044b7daf9800142664e127.westus2.azure.elastic-cloud.com:443",
            "api_key": "VDI2TVlKVUJxbWI1c3FaYTBybkw6Z3EwY2ZuTnFTeUtGejJscWJ6Nmc5dw=="
        }
        # self.es_config = {
        #     "hosts": "https://aielastic.es.westus2.azure.elastic-cloud.com:443",
        #     "api_key": "3cwYZ6d4qSMkLYT663mohAuU"
        # }
        # self.index_name = "web_content"
        # self.index_name = "essex"
        self.index_name = "provider_test"
        # 启动ES写入进程
        self.es_writer_pool = []
        for _ in range(self.process_num):
            self.es_writer = ESWriterProcess(self.es_queue, self.es_config, self.index_name)
            self.es_writer.start()
            self.es_writer_pool.append(self.es_writer)
        
    def process_item(self, item, spider):

        
        # 检查爬取深度
        depth = item.get('depth', 0)
        if depth == 0:
            self.process_count += 1
            print(f"{self.process_count}/{self.total_count}, {item['url']}")
        # print(item['depth'], item['url'], item['name'])
        # print(self.es_queue.qsize())
        
        if self.es_queue.qsize() >= self.max_queue_size:
            print(f"Reach max queue size, block for a while!")
        # # block 避免内存溢出
        self.es_queue.put(item, block=True)
        

        
    def close_spider(self, spider):
        # self._submit_bulk()  # 最后提交剩余数据
        # 通知写入进程关闭
        
        for es_writer in self.es_writer_pool:
            es_writer.shutdown()
            es_writer.join()
            
            if es_writer.is_alive():
                spider.logger.error("ES写入进程未正常终止")
            else:
                spider.logger.info("ES写入进程已正常关闭")