import datetime
import os
import json
import logging
from elasticsearch import Elasticsearch
from dotenv import dotenv_values
import threading

# Global variables
config = None
eslogger = None
logger = None


class ElasticsearchHandler(logging.Handler):
    def __init__(self, index):
        super().__init__()
        self.index = index
        self.client = None
        self.lock = threading.Lock()

    def get_client(self):
        with self.lock:
            if self.client is None:
                es_config = {
                    "hosts": [config.get("ES_HOST")],
                    "api_key": config.get("ES_API_KEY"),
                    "timeout": 30,
                    "max_retries": 3,
                    "retry_on_timeout": True
                }
                self.client = Elasticsearch(**es_config)
            return self.client

    def emit(self, record):
        try:
            log_data = {
                "@timestamp": datetime.datetime.utcfromtimestamp(record.created).isoformat() + "Z",
                "log_level": record.levelname,
                "message": record.getMessage(),
                "logger_name": record.name,
                "module": record.module,
                "file": f"{record.filename}:{record.lineno}"
            }
            # Use a thread to write to Elasticsearch asynchronously
            threading.Thread(target=self._write_to_es, args=(log_data,)).start()
        except Exception as e:
            print(f"准备写入 Elasticsearch 时出错: {e}")
            raise e

    def _write_to_es(self, log_data):
        """执行 Elasticsearch 写入的方法"""
        try:
            es_client = self.get_client()
            es_client.index(index=self.index, body=log_data)
        except Exception as e:
            print(f"写入 Elasticsearch 时出错: {e}")
            raise e


def init_globals(config_file):
    """
    初始化全局变量：加载配置、设置日志
    """
    global eslogger, config, logger
    config = dotenv_values(config_file)

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
    es_handler = ElasticsearchHandler(config.get("ES_LOGGER_INDEX_NAME"))
    logger.addHandler(es_handler)