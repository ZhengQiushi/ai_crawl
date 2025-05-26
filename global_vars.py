import datetime
import os
import json
import logging
from elasticsearch import Elasticsearch
from elasticsearch import AsyncElasticsearch  # Now using async client
from dotenv import dotenv_values
import asyncio

# Global variables
config = None
eslogger = None
logger = None


class AsyncElasticsearchHandler(logging.Handler):
    def __init__(self, index):
        super().__init__()
        self.index = index
        self._async_client = None
        self._loop = asyncio.get_event_loop()
        
    @property
    def async_client(self):
        if self._async_client is None:
            es_config = {
                "hosts": [config.get("ES_HOST")],
                "api_key": config.get("ES_API_KEY"),
                "timeout": 30,
                "max_retries": 3,
                "retry_on_timeout": True
            }
            self._async_client = AsyncElasticsearch(**es_config)
        return self._async_client
    
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
            # Schedule the async task to run in the event loop
            asyncio.run_coroutine_threadsafe(self._async_emit(log_data), self._loop)
        except Exception as e:
            print(f"准备写入 Elasticsearch 时出错: {e}")
            raise e
            
    async def _async_emit(self, log_data):
        """实际执行 Elasticsearch 写入的异步方法"""
        try:
            await self.async_client.index(index=self.index, body=log_data)
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
    es_handler = AsyncElasticsearchHandler(config.get("ES_LOGGER_INDEX_NAME"))
    logger.addHandler(es_handler)
