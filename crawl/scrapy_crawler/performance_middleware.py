import time
import logging
import json
import os
from urllib.parse import urlparse
from scrapy.exceptions import NotConfigured

logger = logging.getLogger(__name__)

class PerformanceMiddleware:
    """
    Scrapy中间件，用于监控爬虫性能
    
    记录每个请求的:
    - 请求时间
    - 下载时间
    - 响应大小
    - 状态码
    - 域名
    """
    
    @classmethod
    def from_crawler(cls, crawler):
        # 检查是否启用了性能监控
        if not crawler.settings.getbool('PERFORMANCE_STATS_ENABLED', True):
            raise NotConfigured
        
        # 获取输出文件路径
        stats_file = crawler.settings.get('PERFORMANCE_STATS_FILE', 'performance_stats.json')
        
        # 创建中间件实例
        return cls(stats_file)
    
    def __init__(self, stats_file):
        self.stats_file = stats_file
        self.stats = {
            'requests': [],
            'domains': {},
            'total_time': 0,
            'total_size': 0,
            'request_count': 0,
            'success_count': 0,
            'failed_count': 0
        }
        self.start_time = time.time()
    
    def process_request(self, request, spider):
        # 记录请求开始时间
        request.meta['start_time'] = time.time()
        return None
    
    def process_response(self, request, response, spider):
        # 计算请求时间
        start_time = request.meta.get('start_time', time.time())
        request_time = time.time() - start_time
        
        # 获取域名
        domain = urlparse(response.url).netloc
        
        # 记录请求统计信息
        request_stats = {
            'url': response.url,
            'status': response.status,
            'size': len(response.body),
            'time': request_time,
            'domain': domain,
            'timestamp': time.time()
        }
        
        # 更新总体统计信息
        self.stats['requests'].append(request_stats)
        self.stats['total_time'] += request_time
        self.stats['total_size'] += len(response.body)
        self.stats['request_count'] += 1
        
        if 200 <= response.status < 400:
            self.stats['success_count'] += 1
        else:
            self.stats['failed_count'] += 1
        
        # 更新域名统计信息
        if domain not in self.stats['domains']:
            self.stats['domains'][domain] = {
                'request_count': 0,
                'total_time': 0,
                'total_size': 0,
                'success_count': 0,
                'failed_count': 0,
                'avg_time': 0
            }
        
        domain_stats = self.stats['domains'][domain]
        domain_stats['request_count'] += 1
        domain_stats['total_time'] += request_time
        domain_stats['total_size'] += len(response.body)
        domain_stats['avg_time'] = domain_stats['total_time'] / domain_stats['request_count']
        
        if 200 <= response.status < 400:
            domain_stats['success_count'] += 1
        else:
            domain_stats['failed_count'] += 1
        
        # 记录慢请求
        if request_time > 5:  # 超过5秒的请求被视为慢请求
            logger.warning(f"慢请求: {response.url} - {request_time:.2f}秒")
        
        return response
    
    def process_exception(self, request, exception, spider):
        # 记录异常
        start_time = request.meta.get('start_time', time.time())
        request_time = time.time() - start_time
        
        domain = urlparse(request.url).netloc
        
        # 记录异常统计信息
        request_stats = {
            'url': request.url,
            'status': 'error',
            'exception': str(exception),
            'time': request_time,
            'domain': domain,
            'timestamp': time.time()
        }
        
        self.stats['requests'].append(request_stats)
        self.stats['total_time'] += request_time
        self.stats['request_count'] += 1
        self.stats['failed_count'] += 1
        
        # 更新域名统计信息
        if domain not in self.stats['domains']:
            self.stats['domains'][domain] = {
                'request_count': 0,
                'total_time': 0,
                'total_size': 0,
                'success_count': 0,
                'failed_count': 0,
                'avg_time': 0
            }
        
        domain_stats = self.stats['domains'][domain]
        domain_stats['request_count'] += 1
        domain_stats['total_time'] += request_time
        domain_stats['failed_count'] += 1
        domain_stats['avg_time'] = domain_stats['total_time'] / domain_stats['request_count']
        
        logger.error(f"请求异常: {request.url} - {exception}")
        
        return None
    
    def spider_closed(self, spider):
        # 计算总运行时间
        self.stats['runtime'] = time.time() - self.start_time
        
        # 计算平均请求时间
        if self.stats['request_count'] > 0:
            self.stats['avg_request_time'] = self.stats['total_time'] / self.stats['request_count']
        else:
            self.stats['avg_request_time'] = 0
        
        # 按平均响应时间排序域名
        sorted_domains = sorted(
            self.stats['domains'].items(),
            key=lambda x: x[1]['avg_time'],
            reverse=True
        )
        
        # 输出性能报告
        logger.info("爬虫性能报告:")
        logger.info(f"总运行时间: {self.stats['runtime']:.2f}秒")
        logger.info(f"总请求数: {self.stats['request_count']}")
        logger.info(f"成功请求数: {self.stats['success_count']}")
        logger.info(f"失败请求数: {self.stats['failed_count']}")
        logger.info(f"平均请求时间: {self.stats['avg_request_time']:.2f}秒")
        
        logger.info("域名性能排名 (按平均响应时间):")
        for domain, stats in sorted_domains[:10]:  # 只显示前10个最慢的域名
            logger.info(f"{domain}: {stats['avg_time']:.2f}秒, {stats['request_count']}个请求")
        
        # 保存统计信息到文件
        os.makedirs('output', exist_ok=True)
        with open(f"output/{self.stats_file}", 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, ensure_ascii=False, indent=2)
        
        logger.info(f"性能统计信息已保存到 output/{self.stats_file}")
    
    def spider_opened(self, spider):
        # 记录爬虫启动时间
        self.start_time = time.time()
        logger.info("性能监控已启动") 