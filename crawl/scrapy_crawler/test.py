import sqlite3
from crawl4ai import PruningContentFilter, DefaultMarkdownGenerator
md_generator = DefaultMarkdownGenerator(content_filter = PruningContentFilter())

db_path = "output/crawl_data.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 查询前5条记录
cursor.execute('SELECT name, url, content FROM pages LIMIT 1')
result = cursor.fetchall()

# 打印结果
for row in result:
    name = row[0]
    url = row[1]
    content = row[2]

    print(f"Name: {name}")
    print(f"URL: {url}")
    print("Filtered Content:")
    print(content)
    

# 关闭连接
conn.close() 
