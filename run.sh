nohup /home/azureuser/miniconda3/envs/email/bin/python /home/azureuser/DataCrawl/multi_crawler/retry_python_lzs_from_es.py --County  "Morris,Essex,Hudson,Union" --State NewJersey --Num 10000  --Config "/home/azureuser/DataCrawl/multi_crawler/crawler_server/config_dev.env" > /home/azureuser/DataCrawl/logs/retry.log 2>&1 &


/home/azureuser/miniconda3/envs/email/bin/python /home/azureuser/ai_crawl/app_from_es.py --County  "Morris,Essex,Hudson,Union" --State NewJersey --Num 10000  --Config "/home/azureuser/ai_crawl/config.env"