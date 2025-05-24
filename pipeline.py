import csv
import json
from bs4 import BeautifulSoup, Comment
from elasticsearch import Elasticsearch, helpers, AsyncElasticsearch
from crawl4ai import PruningContentFilter, DefaultMarkdownGenerator
import hashlib
import global_vars
import time
import re
import global_vars
import logging
from urllib.parse import urljoin
from twisted.internet import defer, threads
import threading
import copy
import datetime

class CsvPipeline:
    def __init__(self):

        es_config = {
            "hosts": global_vars.config.get("ES_HOST"),
            "api_key": global_vars.config.get("ES_API_KEY")
        }
        self.index_name = global_vars.config.get("ES_INDEX_NAME")
        self.logger = global_vars.logger

        # self._buffer = []  # 数据缓冲区，用于暂存待写入的数据
        # self.md_generator = DefaultMarkdownGenerator(content_filter=PruningContentFilter(threshold=0.35))  # 
        
        self.md_generator = DefaultMarkdownGenerator()  # 
        
        # Markdown生成器
        self.es = Elasticsearch(**es_config)  # 创建ES客户端
        self.batch_size = 1
        # self.logger.info("new pipeline!!!")
        self.pipeline_lock = threading.Lock() # 线程锁
    def process_item(self, item, spider):
        self.write(item)
        return item
    
    def filter_html(self, html, domain = ''):
        soup = BeautifulSoup(html, 'lxml')
        if not soup.body:
            soup = BeautifulSoup(f"<body>{html}</body>", "lxml")
        
        # 删除所有注释
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.decompose()  # 或者使用 comment.decompose()
        
        excluded_tags = [
            "nav",
            "footer",
            "header",
            # "aside",
            # "script",
            "br"
            # "style",
            # "form",
            # "iframe",
            # "noscript",
        ]
        
        for tag in excluded_tags:
            for element in soup.find_all(tag):
                element.decompose()
                
        body = soup.find("body")            

        def pure_link(url):
            if len(url) == 0:
                return ''
            if url.startswith('//'):  # 如果以 "//" 开头
                url = 'http:' + url  # 添加 "http:"
            elif domain and not url.startswith(('http://', 'https://')):
                url = urljoin(f"http://{domain}", url)
            return url

        # 提取图片信息并保存到列表
        images = []
        for img in body.find_all('img'):
            if 'src' in img.attrs:  # 确保<img>标签有src属性
                img_url = img['src']
                if img_url.find('base64') >= 0:
                    img.decompose()
                    continue
                if len(img_url) == 0:
                    continue
                img_url = pure_link(img_url)
                images.append(img_url)
            img.decompose()
        
        pdf_links = []

        for a_tag in soup.find_all('a', href=True):  # 查找所有具有 href 属性的 <a> 标签
            href = a_tag['href']
            if href and '.pdf' in href.lower():  # 检查 href 是否包含 "pdf" （不区分大小写）
                pdf_url = pure_link(href)
                pdf_links.append(pdf_url)
        return str(body), images, pdf_links

    def write(self, item):
        # 生成文档ID
        doc_id = item['row']['businessID']
        # 如果 content 是空字符串，则构造一个不包含 pages 的文档
        if not item['row']['content'] or item['row']['content'] == "":
            es_doc = {
                'doc_id': doc_id,
                'script': {
                    "source": """
                        if (ctx._source.pages == null) {
                            ctx._source.pages = new ArrayList();
                        }
                    """,
                    "lang": "painless",
                    "params": {}
                },
                'upsert': {
                    'pages': [],  # 保留 pages 字段为空
                    **{k: item['row'][k] for k in ['state', 'county', 'googleReview', 
                                                'googleReviewRating', 'googleReviewCount',
                                                'domain', 'googleEntry', 'businessFullName', 'businessID', 'website'] if k in item['row']}
                }
            }
        else:
            remaining_content, img_urls, pdf_urls = self.filter_html(item['row']['content'], item['row']['domain'])
            if item['row']['depth'] == 0:
                item['row']['content'] = self.md_generator.generate_markdown(item['row']['content'], item['row']['url']).raw_markdown
            else:
                item['row']['content'] = self.md_generator.generate_markdown(remaining_content, item['row']['url']).raw_markdown

            item['row']['img_urls'] = sorted(list(set(img_urls)))
            item['row']['pdf_urls'] = sorted(list(set(pdf_urls)))

            # 生成内容哈希值
            content_string = item['row']['content']
            image_string = ''.join(item['row']['img_urls'])  # 将图片链接列表转换为字符串
            pdf_string = ''.join(item['row']['pdf_urls'])    # 将 PDF 链接列表转换为字符串

            # 将内容、图片链接、PDF 链接拼接在一起
            combined_string = content_string + image_string + pdf_string + "aaa"

            # 计算哈希值
            content_hash = hashlib.md5(combined_string.encode()).hexdigest()
            # (可选) 将哈希值添加到item中
            item['row']['content_hash'] = content_hash
            current_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # 构造 Elasticsearch 文档
            es_doc = {
                'doc_id': doc_id,
                'script': {
                    "source": """
                        if (ctx._source.pages == null) {
                            ctx._source.pages = new ArrayList();
                        }
                        boolean pageExists = false;
                        boolean isUpdated = false;
                        for (page in ctx._source.pages) {
                            if (page.url == params.url) {
                                pageExists = true;

                                if (page.content_hash != params.content_hash) {
                                    page.title = params.title;
                                    page.content = params.content;
                                    page.img_urls = params.img_urls;
                                    page.pdf_urls = params.pdf_urls;
                                    page.content_hash = params.content_hash;
                                    page.last_modified = params.current_time;
                                    isUpdated = true
                                }
                                break;
                            }
                        }
                        if (!pageExists) {
                            ctx._source.pages.add([
                                'url': params.url,
                                'title': params.title,
                                'content': params.content,
                                'img_urls': params.img_urls,
                                'pdf_urls': params.pdf_urls,
                                'content_hash': params.content_hash,
                                'last_modified': params.current_time
                            ]);
                        }

                        if (isUpdated){
                            ctx._source.refresh_pages_time = params.refresh_pages_time;
                        }
                    """,
                    "lang": "painless",  # Elasticsearch 脚本语言
                    "params": {
                        'url': item['row']['url'],
                        'title': item['row']['title'],
                        'content': item['row']['content'],
                        'img_urls': item['row']['img_urls'],
                        'pdf_urls': item['row']['pdf_urls'],
                        'content_hash': content_hash,
                        'current_time': int(time.time() * 1000),  # 当前时间戳
                        'refresh_pages_time': current_time_str
                    }
                },
                'upsert': {  # 如果文档不存在，则插入新文档
                    'pages': [{
                        'url': item['row']['url'],
                        'title': item['row']['title'],
                        'content': item['row']['content'],
                        'img_urls': item['row']['img_urls'],
                        'pdf_urls': item['row']['pdf_urls'],
                        'content_hash': content_hash,
                        'last_modified': int(time.time() * 1000)
                    }],
                    'refresh_pages_time': current_time_str,
                    **{k: item['row'][k] for k in ['state', 'county', 'googleReview', 
                                                'googleReviewRating', 'googleReviewCount',
                                                'domain', 'googleEntry', 'businessFullName', 'website', 'businessID'] if k in item['row']}
                }
            }
        
        # 将文档添加到缓冲区
        # self._buffer.append(es_doc)
        
        # 如果缓冲区达到批量大小，则写入 Elasticsearch
        # if len(self._buffer) >= self.batch_size:
        self._bulk_write(es_doc, item)
        
        if not item['row']['content'] or item['row']['content'] == "":
            self.logger.warning(f"empty pages: {item['row']['businessID']}")
        else:
            self.logger.info(f"proc {item['row']['processID']} 完成写入, depth={item['row']['depth']}, businessID={item['row']['businessID']}, url={item['row']['url']}")
        del item  # 释放内存

    def _bulk_write(self, doc, item):
        """
        将缓冲区中的数据批量写入 Elasticsearch。
        如果写入失败，会进行重试。
        """
        max_retries = 100  # 最大重试次数
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                actions = []
                # for doc in self._buffer:
                if True:
                    actions.append({
                        "_op_type": "update",  # 更新操作
                        "_index": self.index_name,  # 索引名称
                        "_id": doc['doc_id'],  # 文档ID
                        "script": doc['script'],  # 更新脚本
                        "upsert": doc['upsert']  # 插入数据
                    })
                
                # 使用 helpers.bulk 批量写入
                success, failed = helpers.bulk(
                    self.es, 
                    actions, 
                    stats_only=True,
                    request_timeout=60  # 设置超时时间为60秒
                )
                # self._buffer.clear()  # 清空缓冲区
                break
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    pass
                    # self.logger.warning(f" 写入ES失败，正在重试 ({retry_count}/{max_retries}): proc {item['row']['processID']}, depth={item['row']['depth']}, businessID={item['row']['businessID']}, url={item['row']['url']} \n {str(e)}")
                else:
                    self.logger.error(f" 写入ES失败，已达到最大重试次数: proc {item['row']['processID']}, depth={item['row']['depth']}, businessID={item['row']['businessID']}, url={item['row']['url']} \n {str(e)}")
                    self.logger.exception("详细错误信息:")  # 记录完整的异常堆栈信息
                    # self._buffer.clear()  # 清空缓冲区，避免数据堆积


    def close_spider(self, spider):
        self.logger.info("close pipeline!!!")
        # self._bulk_write()