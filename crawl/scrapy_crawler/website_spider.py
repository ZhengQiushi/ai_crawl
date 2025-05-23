import scrapy
import os
import pandas as pd
from urllib.parse import urlparse, urljoin
import re
import json
from scrapy.linkextractors import LinkExtractor
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from crawl4ai import PruningContentFilter, DefaultMarkdownGenerator

class WebsiteSpider(CrawlSpider):
    name = 'website_spider'
    MAX_LINKS_PER_PAGE = 500  # 每个页面最多爬取10个链接
    # 文件类型过滤
    excluded_extensions = ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', 
                          '.zip', '.rar', '.jpg', '.jpeg', '.png', '.gif', '.mp3', 
                          '.mp4', '.avi', '.mov', '.ics', '.ical']
    excluded_formats = ['format=ical', 'format=ics', 'format=pdf']
    
    # 定义爬取规则
    rules = (
        # 提取链接并跟随，使用process_links方法过滤链接
        Rule(LxmlLinkExtractor(unique=True), callback='parse_item', follow=True, process_links='filter_links', process_request='process_request'),
    )
    
    def __init__(self, *args, **kwargs):
        # 从CSV文件读取URL
        self.csv_file = kwargs.get('csv_file', '')
        self.urls = []  # 确保初始化为空列表
        self.md_generator = DefaultMarkdownGenerator(content_filter = PruningContentFilter())
        
        # 如果提供了CSV文件，读取URL
        if self.csv_file and os.path.exists(self.csv_file):
            self.read_urls_from_csv(self.csv_file)
        else: 
            # 如果直接提供了URL列表
            urls = kwargs.get('urls', [])
            if urls:
                if isinstance(urls, str):
                    self.urls = [urls]
                else:
                    self.urls = list(urls)
                self.names = self.urls
                self.df = None
        
        # 设置允许的域名
        self.allowed_domains_list = []
        for url in self.urls:
            domain = urlparse(url).netloc
            if domain and domain not in self.allowed_domains_list:
                self.allowed_domains_list.append(domain)
        
        # 设置起始URL
        self.start_urls = self.urls
        
        super(WebsiteSpider, self).__init__(*args, **kwargs)
    
    def filter_links(self, links):
        """使用is_valid_url方法过滤链接"""
        valid_links = [link for link in links if self.is_valid_url(link.url) and re.search(r'[a-zA-Z]', link.text.strip())]
        # if len(valid_links) > self.MAX_LINKS_PER_PAGE:
        #     print(f"{valid_links[0]}, reach max links per page: {len(valid_links)}")
        # for link in links:
        #     if not link.text:
        #         print(link)
        # print(len(valid_links))
        return valid_links  # 只返回前N个有效链接
    
    def read_urls_from_csv(self, filename):
        """从CSV文件读取URL"""
        try:
            self.df = pd.read_csv(filename, nrows=None)
            self.df = self.df[self.df['Website'].notna()]
            self.df = self.df[self.df['GT planning'].notna()]
            # self.df = self.df[self.df['Name'].str.contains("LearningRx")]
            self.df = self.df.fillna('')
            print(self.df)
            
            # 检查是否存在'Website'列
            if 'Website' not in self.df.columns:
                print(f"错误: CSV文件 {filename} 不包含'Website'列")
                return
            
            # 从Website列提取URL，移除空值
            # urls = df['Website'].dropna().tolist()
            self.urls = self.df['Website'].tolist()
            self.names = self.df['Name'].tolist()
            
            print(f"从CSV中读取了 {len(self.urls)} 个URL")
        except Exception as e:
            print(f"读取CSV文件 {filename} 时出错: {str(e)}")
    
    def is_valid_url(self, url):
        """检查URL是否有效且适合爬取"""
        
        # 排除特定格式的文件
        for ext in self.excluded_extensions:
            if url.lower().endswith(ext):
                return False
        
        # 检查URL是否包含排除的格式参数
        for fmt in self.excluded_formats:
            if fmt in url.lower():
                return False
        
        # 检查域名是否在允许的域名列表中或是其子域名
        url_domain = urlparse(url).netloc
        if not url_domain:
            return False
        
        # 检查是否为允许的域名或子域名
        for allowed_domain in self.allowed_domains_list:
            # 完全匹配
            if url_domain == allowed_domain:
                return True
            # 子域名匹配 (例如 blog.example.com 是 example.com 的子域名)
            if url_domain.find(allowed_domain) >= 0 or allowed_domain.find(url_domain) >= 0:
                return True
        
        # print(f"Domain not allowed: {url_domain}")  # 添加日志
        return False
    
    def start_requests(self):
        for i, url in enumerate(self.start_urls):
            yield scrapy.Request(
                url,
                dont_filter=False,
                meta={
                    'idx': i,
                    'base_url': url,
                    'name': self.names[i],
                    'row_data': self.df.iloc[i].to_dict() if self.df else None
                }
            )
            
    def parse_start_url(self, response):
        """处理起始URL的响应"""
        return self.parse_item(response)
    
    def process_request(self, request, response):
        request.meta['base_url'] = response.meta['base_url']
        request.meta['name'] = response.meta['name']
        request.meta['row_data'] = response.meta['row_data']
        return request
    
    
    def parse_item(self, response):
        """处理每个页面"""
        url = response.url
        try:
            content = response.body.decode(response.encoding or 'utf-8')
        except:
            return
        idx = response.meta.get('idx', 0)
        
        # 检查爬取深度
        depth = response.meta.get('depth', 0)
        # 获取页面内容
        base_url = response.meta.get('base_url', '')
        name = response.meta.get('name', '')
        # 提取页面标题
        title = response.css('title::text').get() or ''
        row_data = response.meta.get('row_data', {})
            
        # 返回结果
        # yield {
        #     'name': name,
        #     'url': url,
        #     'title': title,
        #     'base_url': base_url,
        #     'content': content,
        #     'depth': depth,
        #     'row_data': row_data
        # }
    
    def closed(self, reason):
        """爬虫关闭时的处理"""
        print(f"爬虫完成，原因: {reason}")