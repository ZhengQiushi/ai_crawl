# Scrapy网站爬虫

这是一个使用Scrapy框架实现的网站爬虫，可以从CSV文件中读取URL列表，爬取网站内容，并将结果保存到文件中。

## 功能特点

- 从CSV文件读取URL列表
- 爬取网站内容，包括文本和链接
- 支持设置爬取深度和并发数
- 自动过滤无效URL和特定文件类型
- 性能监控，记录请求时间和响应大小
- 生成详细的性能报告
- 将爬取结果保存为文本文件和CSV文件

## 安装依赖

```bash
pip install scrapy pandas
```

## 文件结构

- `website_spider.py`: 爬虫主类，实现爬取逻辑
- `run_crawler.py`: 运行脚本，用于启动爬虫
- `performance_middleware.py`: 性能监控中间件，记录请求性能

## 使用方法

### 从CSV文件爬取URL

```bash
python run_crawler.py path/to/your/file.csv
```

CSV文件应包含一个名为`Website`的列，其中包含要爬取的URL。

### 直接爬取指定URL

修改`run_crawler.py`中的代码，直接提供URL列表：

```python
# 运行爬虫
run_crawler(urls=['https://example.com', 'https://example.org'], settings=settings)
```

## 配置选项

在`run_crawler.py`中，您可以修改以下设置：

- `CONCURRENT_REQUESTS`: 并发请求数（默认16）
- `DOWNLOAD_DELAY`: 下载延迟（默认0.5秒）
- `DEPTH_LIMIT`: 爬取深度限制（默认3）
- `DOWNLOAD_TIMEOUT`: 下载超时（默认30秒）

在`website_spider.py`中，您可以修改：

- `max_depth`: 爬取深度（默认3）
- `excluded_extensions`: 排除的文件扩展名
- `excluded_formats`: 排除的URL格式

## 性能监控

爬虫会自动记录性能数据，并在爬取完成后生成性能报告。报告包括：

- 总运行时间
- 总请求数
- 成功和失败的请求数
- 平均请求时间
- 按响应时间排序的域名列表

性能数据将保存在`output/performance_stats.json`文件中。

## 输出文件

- `data/web/{domain}.txt`: 每个网站的爬取内容
- `output/crawl_summary.xlsx`: 爬取摘要
- `{csv_file}_with_content.csv`: 更新后的CSV文件，包含爬取内容

## 网络瓶颈分析

如果您想分析网络瓶颈，可以查看性能报告中的以下信息：

1. 慢请求（超过5秒的请求）会在日志中标记出来
2. 性能报告中会列出按平均响应时间排序的域名
3. `performance_stats.json`文件包含每个请求的详细信息

通过分析这些数据，您可以找出：

- 响应时间最慢的网站
- 请求失败率最高的网站
- 下载大小最大的网站

这些信息可以帮助您优化爬虫配置，例如增加特定网站的超时时间或减少并发请求数。 