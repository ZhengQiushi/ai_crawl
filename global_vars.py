import datetime
import os
import json
import logging
from elasticsearch import Elasticsearch # , AsyncElasticsearch
from dotenv import dotenv_values
from kafka_wrapper import KafkaConsumerThread, KafkaProducerWrapper
from scrapy.utils.log import configure_logging
import threading
from twisted.internet import threads

# 全局变量
config = None
producer = None
eslogger = None
kafka_producer = None
logger = None
metrics_monitor = None
reactor_queue = None

REACTOR_QUEUE_LEN = 100

reactor_semaphore = None

# 自定义 Elasticsearch 处理程序
class AsyncElasticsearchHandler(logging.Handler):
    def __init__(self, index):
        super().__init__()
        self.index = index

    def emit(self, record):
        try:
            log_data = {
                "@timestamp": datetime.datetime.utcfromtimestamp(record.created).isoformat() + "Z",
                "log_level": record.levelname,  # 日志级别
                "message": record.getMessage(),  # 日志消息
                "logger_name": record.name,  # 日志器名称
                "module": record.module,  # 模块名称
                "file": f"{record.filename}:{record.lineno}"
            }
            # 使用 deferToThread 异步写入 Elasticsearch
            deferred = threads.deferToThread(self._async_emit, log_data)
            deferred.addErrback(self._handle_error)
        except Exception as e:
            print(f"准备写入 Elasticsearch 时出错: {e}")
            raise e
            
    def _async_emit(self, log_data):
        """实际执行 Elasticsearch 写入的方法"""
        try:
            eslogger.index(index=self.index, body=log_data)
        except Exception as e:
            print(f"写入 Elasticsearch 时出错: {e}")
            raise e
            
    def _handle_error(self, failure):
        """处理异步写入失败"""
        print(f"异步写入 Elasticsearch 失败: {failure.getErrorMessage()}")
        return failure

def init_globals(config_file):
    """
    初始化全局变量：加载配置、设置日志、创建 Kafka Producer 实例
    """
    global eslogger, config, kafka_producer, logger, metrics_monitor, reactor_semaphore
    
    reactor_semaphore = threading.Semaphore(REACTOR_QUEUE_LEN)


    config = dotenv_values(config_file)

    es_config = {
        "hosts": [config.get("ES_HOST")],
        "api_key": config.get("ES_API_KEY"),
        # 其他可选配置
        "timeout": 30,
        "max_retries": 3,
        "retry_on_timeout": True
    }

    try:
        eslogger = Elasticsearch(**es_config)
        # 测试连接
        if not eslogger.ping():
            raise ValueError("无法连接到 Elasticsearch")
        print("日志成功连接到 Elasticsearch")
    except Exception as e:
        print(f"Elasticsearch 连接错误: {str(e)}")
    except ValueError as e:
        print(str(e))
    
    
    
    # 配置日志输出
    
    formatter = logging.Formatter('%(asctime)s %(filename)s:%(lineno)d [%(levelname)s] %(message)s')
    logger = logging.getLogger("ai_parsing")
    logger.setLevel(config.get('ES_LOGGER_LEVEL'))
    
    
    log_file = config.get("ES_LOGGER_PATH")
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


    # 创建 Elasticsearch 日志处理器
    es_handler = AsyncElasticsearchHandler(config.get("ES_LOGGER_INDEX_NAME"))
    logger.addHandler(es_handler)
    

    # 配置Scrapy日志
    configure_logging(install_root_handler=False)

    # 初始化 Kafka 生产者
    kafka_producer = KafkaProducerWrapper(config['KAFKA_BOOTSTRAP_SERVERS'], config['KAFKA_PASSWORD'])


