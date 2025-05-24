nohup /home/azureuser/miniconda3/envs/email/bin/python /home/azureuser/ai_crawl/app_from_es.py --County  "Morris,Essex,Hudson,Union" --State NewJersey --Num 10000  --Config "/home/azureuser/ai_crawl/config.env" > /home/azureuser/ai_crawl/logs/retry.log 2>&1 &


/home/azureuser/miniconda3/envs/email/bin/python /home/azureuser/ai_crawl/app_from_es.py --County  "Morris,Essex,Hudson,Union" --State NewJersey --Num 10000  --Config "/home/azureuser/ai_crawl/config.env"