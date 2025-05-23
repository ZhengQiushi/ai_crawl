import os
import sys
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from website_spider import WebsiteSpider
from performance_middleware import PerformanceMiddleware

# 设置爬虫参数
def run_crawler(csv_file=None, urls=None, settings=None):
    """
    运行爬虫
    
    参数:
    - csv_file: CSV文件路径，包含要爬取的URL列表
    - urls: 直接提供的URL列表
    - settings: 爬虫设置
    """
    if settings is None:
        settings = {}
    
    # 创建爬虫进程
    process = CrawlerProcess(settings)
    
    # 确保urls参数不为None
    if urls is None:
        urls = []
    
    # 添加爬虫
    process.crawl(
        WebsiteSpider,
        csv_file=csv_file,
        urls=urls
    )
    
    # 启动爬虫
    process.start()

if __name__ == "__main__":
    # 默认设置
    settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
        'ROBOTSTXT_OBEY': True,  # 关闭robots.txt检查
        'CONCURRENT_REQUESTS': 512,  # 并发请求数
        'CONCURRENT_REQUESTS_PER_DOMAIN': 512,  # 默认为8
        'DOWNLOAD_DELAY': 0,  # 下载延迟
        'COOKIES_ENABLED': False,
        'TELNETCONSOLE_ENABLED': False,
        'LOG_LEVEL': 'DEBUG',
        'RETRY_TIMES': 3,
        'RETRY_ENABLED': True,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 423],
        'HTTPCACHE_ENABLED': True  ,
        'HTTPCACHE_EXPIRATION_SECS': 3600,  # 缓存过期时间（24小时）
        'HTTPCACHE_IGNORE_HTTP_CODES': [500, 502, 503, 504, 408, 423],
        'DEPTH_LIMIT': 2,  # 爬取深度限制
        'DEPTH_PRIORITY': 0,  # 深度优先
        # 'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleLifoDiskQueue',
        # 'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.LifoMemoryQueue',
        
        # 性能监控设置
        'PERFORMANCE_STATS_ENABLED': True,
        'PERFORMANCE_STATS_FILE': 'performance_stats.json',
        
        # 启用性能监控中间件
        'DOWNLOADER_MIDDLEWARES': {
            # 'performance_middleware.PerformanceMiddleware': 543,
            'scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware': 500,
        },
        
        'DUPEFILTER_CLASS': 'dupefilter.EntranceAwareDupeFilter',
        
        # 'ITEM_PIPELINES': {
        #     'pipeline.WebsitePipeline': 300,
        # },
        
        # 启用扩展
        'EXTENSIONS': {
            'scrapy.extensions.telnet.TelnetConsole': None,
            'scrapy.extensions.logstats.LogStats': 0,
        },
        
        # 下载超时设置
        'DOWNLOAD_TIMEOUT': 30,  # 30秒超时
        
        # 自动限速设置
        # 'AUTOTHROTTLE_ENABLED': True,
        # 'AUTOTHROTTLE_START_DELAY': 5.0, # 初始下载延迟
        # 'AUTOTHROTTLE_MAX_DELAY': 60.0, # 最大下载延迟
        # 'AUTOTHROTTLE_TARGET_CONCURRENCY': 64, # 目标并发数
        # 'AUTOTHROTTLE_DEBUG': False, # 关闭debug模式
    }
    
    # 从命令行参数获取CSV文件路径
    csv_file = None
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
        if not os.path.exists(csv_file):
            print(f"错误: CSV文件 {csv_file} 不存在")
            sys.exit(1)
    urls = [
        'http://www.centercourtacademy.com/'
    ]
    # 对URLs进行去重处理
    urls = list(set(urls))

    run_crawler(csv_file=csv_file, settings=settings, urls=urls) 