nohup /usr/bin/python3 /root/ai_crawl/app_from_es.py --County  "Morris,Essex,Hudson,Union" --State NewJersey --Num 10000  --Config "/root/ai_crawl/config.env" > /root/ai_crawl/logs/retry.log 2>&1 &


/usr/bin/python3 /root/ai_crawl/app_from_es.py --County  "Morris,Essex,Hudson,Union" --State NewJersey --Num 10000  --Config "/root/ai_crawl/config.env"


/usr/bin/python3 /root/ai_crawl/app_from_local.py --County  "Morris,Essex,Hudson,Union" --State NewJersey --Num 10000  --Config "/root/ai_crawl/config.env"


/usr/bin/python3 /root/ai_crawl/app.py --County  "Morris,Essex,Hudson,Union" --State NewJersey --Num 10000  --Config "/root/ai_crawl/config.env"

nohup /usr/bin/python3 /root/ai_crawl/app_from_local.py --County  "Morris,Essex,Hudson,Union" --State NewJersey --Num 10000  --Config "/root/ai_crawl/config.env" > /root/ai_crawl/logs/retry_local.log 2>&1 &

