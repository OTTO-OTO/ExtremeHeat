# 付费
import json
import re
import time
import uuid
import random
import os

import requests
import feapder
from feapder import Item
from feapder.db.mysqldb import MysqlDB
from feapder.network.downloader._requests import RequestsDownloader
from feapder.network.response import Response

# 自定义下载器，确保使用正确的代理设置
class CustomRequestsDownloader(RequestsDownloader):
    def download(self, request):
        # 使用请求中指定的代理设置
        kwargs = request.requests_kwargs.copy()
        # 确保代理设置存在
        if 'proxies' not in kwargs:
            # 从环境变量获取代理设置
            import os
            proxy = os.environ.get('http_proxy', 'http://127.0.0.1:7897')
            kwargs['proxies'] = {
                'http': proxy,
                'https': os.environ.get('https_proxy', proxy)
            }
        
        # 添加重试机制
        max_retries = 3
        for retry in range(max_retries):
            try:
                # 添加随机延迟
                time.sleep(random.uniform(1, 3))
                
                # 确保请求头存在
                if 'headers' not in kwargs:
                    kwargs['headers'] = {}
                
                # 确保User-Agent存在
                if 'User-Agent' not in kwargs['headers']:
                    kwargs['headers']['User-Agent'] = random.choice([
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0"
                    ])
                
                # 确保Referer存在
                if 'Referer' not in kwargs['headers'] and 'referer' not in kwargs['headers']:
                    kwargs['headers']['Referer'] = "https://www.google.com/"
                
                # 发送请求
                response = requests.request(
                    request.method, request.url, **kwargs
                )
                
                # 检查是否被阻止
                if response.status_code == 403:
                    print(f"403 Forbidden - Retry {retry+1}/{max_retries}")
                    # 更换User-Agent
                    kwargs['headers']['User-Agent'] = random.choice([
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/537.36",
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0"
                    ])
                    # 更换Referer
                    kwargs['headers']['Referer'] = "https://www.google.com/search?q=site:lahora.com.ec"
                    # 增加延迟
                    time.sleep(random.uniform(3, 5))
                    continue
                
                # 检查响应状态码
                if response.status_code == 200:
                    response = Response(response)
                    return response
                else:
                    print(f"Unexpected status code: {response.status_code} - Retry {retry+1}/{max_retries}")
                    time.sleep(random.uniform(2, 4))
                    continue
            except Exception as e:
                print(f"Download error: {str(e)} - Retry {retry+1}/{max_retries}")
                time.sleep(random.uniform(2, 5))
                continue
        
        # 所有重试都失败，返回一个空的Response对象
        import http.client
        from io import BytesIO
        
        # 创建一个空的HTTP响应
        mock_response = http.client.HTTPResponse(BytesIO())
        mock_response.status = 403
        mock_response.reason = "Forbidden"
        mock_response.headers = {}
        
        # 创建一个Requests响应
        requests_response = requests.Response()
        requests_response.status_code = 403
        requests_response._content = b"403 Forbidden"
        requests_response.headers = {}
        requests_response.url = request.url
        
        # 创建并返回一个Response对象
        response = Response(requests_response)
        return response


class AirSpiderDemo(feapder.AirSpider):
    # 数据库连接和表结构创建
    db = MysqlDB(
        ip="192.168.42.183", port=3306, db="spider_data", user_name="czm", user_pass="root"
    )
    mysql_db = "spider_data"
    table = "Ecuador"
    
    # 创建表结构
    create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS `{mysql_db}`.`{table}`  (
                  `id` int NOT NULL AUTO_INCREMENT,
                  `title` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '标题',
                  `author` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '作者',
                  `keyword` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '关键词',
                  `content` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '内容',
                  `article_url` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '文章网址',
                  `pubtime` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '发布时间',
                  `country` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '国家',
                  `news_source_country` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '新闻来源国家',
                  `place` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '地名',
                  `Longitude_latitude` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '经纬度',
                  `english` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '英文',
                  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '写入时间',
                  PRIMARY KEY (`id`) USING BTREE,
                  UNIQUE INDEX `title_uni`(`keyword` ASC, `article_url` ASC) USING BTREE
                ) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

                    """
    db.execute(create_table_sql)
    print(f"{table}创建成功<=================")
    
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，改为5线程以提高爬取速度
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[10, 15],  # 调整请求间隔，平衡速度和被封禁的风险
        SPIDER_MAX_RETRY_TIMES=3,  # 增加重试次数
        MYSQL_IP="192.168.42.183",
        MYSQL_PORT=3306,
        MYSQL_DB="spider_data",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=False,  # 禁用 item 去重，使用数据库唯一索引确保不重复
        # 使用自定义下载器
        DOWNLOADER="__main__.CustomRequestsDownloader",
    )

    country = 'Ecuador'
    table = 'Ecuador'
    
    def __init__(self):
        super().__init__()
        self.previous_links = {}  # 每个关键词独立的链接跟踪

    # 随机User-Agent列表
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0"
    ]
    
    # 西班牙语关键词
    keywords = ["Extremo", "Calor", "Alta", "Temperatura", "Lluvia", "Pesada", "Sequía", "Poder", "Corte", "de", "Electricidad", "debido", "al", "calor", "Incendio", "Contaminación", "del", "aire", "Cambio", "climático", "Reducción", "de", "los", "rendimientos", "agrícolas", "Hipoxia", "Ataque", "de", "calor", "Impacto", "del", "calor", "en", "el", "tráfico", "Desastre", "ecológico", "Impacto", "del", "cambio", "climático", "en", "la", "economía", "Ola", "de", "calor", "marina", "Contaminación", "relacionada", "con", "el", "calor", "Coral"]

    def start_requests(self):
        # 对每个关键词执行搜索
        for keyword in self.keywords:
            # 随机延迟
            time.sleep(random.uniform(1, 3))
            
            # 固定代理设置
            proxy = "http://127.0.0.1:7897"
            proxies = {
                'http': proxy,
                'https': proxy
            }
            
            # 打印调试信息
            print(f"Using proxy: {proxy}")
            print(f"Proxy settings: {proxies}")
            
            # 使用Google搜索URL，避免直接访问网站被阻止
            url = f"https://www.google.com/search?q=site:lahora.com.ec+{keyword}"
            
            # 随机选择User-Agent
            user_agent = random.choice(self.user_agents)
            
            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "es-ES,es;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                "referer": "https://www.google.com/",
                "sec-ch-ua": f"\"Microsoft Edge\";v=\"{random.randint(130, 135)}\", \"Chromium\";v=\"{random.randint(130, 135)}\", \"Not_A Brand\";v=\"24\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "same-origin",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": user_agent
            }
            
            # 动态生成cookies
            cookies = {
                "_ga": f"GA1.1.{random.randint(100000000, 999999999)}.{random.randint(1000000000, 9999999999)}",
                "_gid": f"GA1.2.{random.randint(100000000, 999999999)}.{random.randint(1000000000, 9999999999)}",
                "_gat": f"{random.randint(1, 10)}",
                "session": f"{uuid.uuid4()}",
                "cookie_consent": "true"
            }
            
            # 设置请求参数，明确指定代理
            requests_kwargs = {
                'headers': headers,
                'cookies': cookies,
                'timeout': 30,
                'verify': False,
                'proxies': proxies
            }
            
            print(f"搜索关键词: {keyword}")
            yield feapder.Request(url, callback=self.parse_url, page=0, keyword=keyword, requests_kwargs=requests_kwargs, filter_repeat=True)

    def download_midware(self, request):
        # 随机选择User-Agent
        user_agent = random.choice(self.user_agents)
        
        # 随机延迟
        time.sleep(random.uniform(1, 3))
        
        # 完全重写请求头
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "referer": "https://www.lahora.com.ec/",
            "sec-ch-ua": f"\"Microsoft Edge\";v=\"{random.randint(130, 135)}\", \"Chromium\";v=\"{random.randint(130, 135)}\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "script",
            "sec-fetch-mode": "no-cors",
            "sec-fetch-site": "cross-site",
            "user-agent": user_agent
        }
        
        # 固定代理设置
        proxy = "http://127.0.0.1:7897"
        proxies = {
            'http': proxy,
            'https': proxy
        }
        
        # 打印调试信息
        print(f"Using proxy: {proxy}")
        print(f"Proxy settings: {proxies}")
        
        # 动态生成cookies
        cookies = {
            "__Secure-3PSID": f"g.a000rggyq3rN6l5a4RzrqfKR8L6w_4oSKSt2-NpZPRMnn-bzQbIYlvrEO6wyUoA2X7nR4dKaLQACgYKAc8SARcSFQHGX2Mi1LRifzVo7z1Yh0GO1dLi3BoVAUF8yKq57cjKXq68AYkR7eUXDFJW{random.randint(100, 999)}",
            "__Secure-3PAPISID": f"IMadT4_VMCUqIjYe/AiQnZJDT7r7FsfPRc",
            "__Secure-3PSIDTS": f"sidts-CjIBmiPuTdxxH6Xb01X7UHquAO1qi0ll-YJl7ZQicoz5mw2YUcxvxSUKtLhh5N-aooWcuRAA",
            "NID": f"520=blscRFb8BYbyg_sKToniKO8dwjb7OYxg4O19QrLqqYhdx72HuBYW4aIUU7VO7d2CZxPr2YMN8BePTMmKC1QLMs8fb2BXzQiNf3hSftQORf958UbiW-8R0uspHlNrr0v-p1K8cqhQGCAkcaPG2wFfFLaUx2_hytgvTs_a2PBzc4Y6vSkdQKFNMtbeZ9QPci89fE9ERhWnIVX7ksztDXzqWH_bpC4jPrd9rM5QAFFq-wPaA7M52tL2SKuh5Hach6jn2KZVynJKGBOEzjgv6coRfSukJkcW4z7My8rQhQjq73zmWeurei7dafh-x6zqYFSYM8oBsYKoXnLold5k0LKaYGfpEKD9-oXFIqOq0Y66qjUy7N0hG8IRyAWTNVrxx1e7_l31Vt7ss1LMOWAf0xgfXdRJ8ZnsesoLubpNnMLYHdyl65WYspNolRpD9qAWbSkr-jS3bcPk6qlMy8Hspohlbs6qEUzv5kZa8YYpcZDkU9CDUnccGhADcrniN5oYUndjE6I7xblGyLZKmRFfapzIZnr84ca39K7ak3sn5Ws5xi4V3AHFNZ4STgCeTnjp7N5RAzIXREyQGUmJRe0uwOcgCqxGwsmXruJHgbm3T1H1l03Lk2TW5qcgxBOF_Y7IbyiJHXmgGzT-9T-L_t1pFO3IAwLwAJNEYwrG2G9Fb4_EWLt9ZIsLsSFZYcYbMgvvfZL0ycyhr7iRoXnQ-L6ULl33IXzKqc0hQJ15yPOvB04smxlQ4nX0o1ZFG_KIfBI5UGsNSWuFmHaYU8NfaanJSC41ZG1T9L2nqRlUIk_1lcahQqu4yJLNy7Rml5sFf-Vd4nIJVDOo2GYiM4Oa_nJo",
            "__Secure-3PSIDCC": f"AKEyXzVzq6iCbQRofXwPrP7e_oY2jL4Io7x4zFpS8mMR8KBEtu26XeaTUSVtOUljVtRg8QkFaw4"
        }
        
        # 设置请求参数
        if not hasattr(request, 'requests_kwargs'):
            request.requests_kwargs = {}
        
        # 设置请求参数，明确指定代理
        request.requests_kwargs = {
            'params': request.params if hasattr(request, 'params') else {},
            'headers': request.headers,
            'cookies': cookies,
            'timeout': 22,
            'stream': True,
            'verify': False,
            'proxies': proxies,  # 明确设置代理
        }
        
        # 确保requests库使用指定的代理
        session = requests.Session()
        session.proxies = proxies
        
        return request

    def parse_url(self, request, response):
        """处理搜索结果页面，提取与关键词相关的新闻链接"""
        current_keyword = request.keyword
        current_page = request.page
        
        try:
            # 检查响应状态码
            print(f"关键词 {current_keyword} 第 {current_page} 页响应状态码: {response.status_code}")
            
            # 检查响应内容是否为空
            if not response.text or len(response.text) < 100:
                print(f"响应内容过短，可能是重定向或错误")
                return None
            
            # 检查是否遇到 AWS WAF 验证码
            if 'awsWafCookieDomainList' in response.text or response.status_code == 202:
                print(f"检测到 AWS WAF 验证码")
                return None
            
            if response.status_code == 403:
                print(f"关键词 {current_keyword} 遇到403错误")
                return None
            
            # 保存页面内容到文件，用于调试
            if current_page == 0:
                with open(f"search_results_{current_keyword}.html", "w", encoding="utf-8") as f:
                    f.write(response.text)
                print(f"已保存搜索结果页面到 search_results_{current_keyword}.html")
            
            # 解析Google搜索结果 - 尝试多种XPath选择器
            link_selectors = [
                "//div[@class='g']//a/@href",
                "//div[@class='tF2Cxc']//a/@href",
                "//h3[@class='LC20lb']/../@href",
                "//a[contains(@href, 'lahora.com.ec')]/@href",
                "//div[@class='rc']//a/@href",
                "//div[@class='srg']//a/@href",
                "//div[@class='r']//a/@href",
                "//a[@jsname='UWckNb']/@href",
                "//a[@class='wIkNI']/@href"
            ]
            
            links = []
            for selector in link_selectors:
                try:
                    extracted_links = response.xpath(selector).extract()
                    if extracted_links:
                        links.extend(extracted_links)
                        print(f"XPath选择器 {selector} 提取到 {len(extracted_links)} 个链接")
                except Exception as e:
                    print(f"XPath选择器 {selector} 出错: {str(e)}")
                    continue
            
            # 打印所有提取到的链接
            print(f"共提取到 {len(links)} 个原始链接")
            for link in links[:10]:  # 只打印前10个链接
                print(f"原始链接: {link}")
            
            # 过滤链接，只保留lahora.com.ec的链接
            filtered_links = []
            for link in links:
                # 处理Google的重定向链接
                if link.startswith('/url?q='):
                    import urllib.parse
                    parsed_url = urllib.parse.urlparse(link)
                    query_params = urllib.parse.parse_qs(parsed_url.query)
                    if 'q' in query_params:
                        actual_url = query_params['q'][0]
                        # 确保链接是完整的URL
                        if not actual_url.startswith('http'):
                            actual_url = 'https://www.lahora.com.ec' + actual_url
                        if 'lahora.com.ec' in actual_url:
                            filtered_links.append(actual_url)
                elif link.startswith('http') and 'lahora.com.ec' in link:
                    filtered_links.append(link)
                elif link.startswith('/'):
                    # 处理相对链接
                    actual_url = 'https://www.lahora.com.ec' + link
                    filtered_links.append(actual_url)
            
            # 去重
            filtered_links = list(set(filtered_links))
            
            # 检查是否与上一页链接相同（用于分页结束判断）
            if current_keyword in self.previous_links and filtered_links == self.previous_links[current_keyword]:
                print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，停止爬取")
                return None
            
            self.previous_links[current_keyword] = filtered_links
            
            if not filtered_links:
                print(f"关键词 {current_keyword} 的第 {current_page} 页没有数据")
                return None
            
            print(f"关键词 {current_keyword} 第 {current_page} 页找到 {len(filtered_links)} 个链接")
                
        except Exception as e:
            print(f"关键词 {current_keyword} 解析出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

        # 为每个链接创建请求，使用当前关键词（确保相关性）
        for link in filtered_links:
            items = Item()
            items.table_name = self.table
            items.article_url = link
            items.title = ""
            items.country = self.country
            items.keyword = current_keyword  # 使用当前搜索的关键词，确保相关性
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)
        
        # 继续爬取下一页（如果有的话）
        next_page = current_page + 1
        
        # 固定代理设置
        proxy = "http://127.0.0.1:7897"
        proxies = {
            'http': proxy,
            'https': proxy
        }
        
        # 随机选择User-Agent
        user_agent = random.choice(self.user_agents)
        
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "es-ES,es;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "referer": f"https://www.google.com/search?q=site:lahora.com.ec+{current_keyword}",
            "sec-ch-ua": f"\"Microsoft Edge\";v=\"{random.randint(130, 135)}\", \"Chromium\";v=\"{random.randint(130, 135)}\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": user_agent
        }
        
        # 动态生成cookies
        cookies = {
            "_ga": f"GA1.1.{random.randint(100000000, 999999999)}.{random.randint(1000000000, 9999999999)}",
            "_gid": f"GA1.2.{random.randint(100000000, 999999999)}.{random.randint(1000000000, 9999999999)}",
            "_gat": f"{random.randint(1, 10)}",
            "session": f"{uuid.uuid4()}",
            "cookie_consent": "true"
        }
        
        # 构造下一页URL
        next_page_url = f"https://www.google.com/search?q=site:lahora.com.ec+{current_keyword}&start={next_page * 10}"
        
        # 设置请求参数，明确指定代理
        requests_kwargs = {
            'headers': headers,
            'cookies': cookies,
            'timeout': 30,
            'verify': False,
            'proxies': proxies
        }
        
        print(f"关键词 {current_keyword} 继续爬取第 {next_page} 页")
        yield feapder.Request(next_page_url, callback=self.parse_url, page=next_page, keyword=current_keyword, requests_kwargs=requests_kwargs, filter_repeat=True)

    def parse_detail(self, request, response):
        try:
            items = request.items
            items.table_name = self.table
            
            # 检查响应状态码
            if hasattr(response, 'status_code') and response.status_code == 202:
                print(f"URL {items.article_url} 返回 202 状态码，可能被反爬")
                return None
            
            # 检查响应内容是否为空
            if not hasattr(response, 'text') or not response.text:
                print(f"URL {items.article_url} 响应内容为空")
                return None
            
            # 检查响应内容长度
            if len(response.text) < 1000:
                print(f"URL {items.article_url} 响应内容过短，可能被反爬")
                return None
            
            # 尝试提取标题
            title_selectors = [
                "//title/text()",
                "//h1/text()",
                "//h1[@class='title']/text()",
                "//h1[@class='article-title']/text()",
                "//div[@class='headline']/h1/text()",
                "//header//h1/text()",
                "//article//h1/text()",
                "//div[@class='entry-title']/h1/text()",
                "//div[@class='post-title']/h1/text()",
                "//div[@class='news-title']/h1/text()"
            ]
            
            title = ""
            for selector in title_selectors:
                try:
                    extracted_titles = response.xpath(selector).extract()
                    if extracted_titles:
                        # 清理HTML标签和多余空白
                        title = re.sub(r'<[^>]+>', '', extracted_titles[0]).strip()
                        title = re.sub(r'\s+', ' ', title)
                        # 去除可能的引号
                        title = title.strip('"')
                        if title:
                            break
                except Exception as e:
                    print(f"提取标题时出错: {str(e)}")
            
            if title:
                items.title = title
            else:
                items.title = "No title"
            
            # 尝试提取内容，使用多个选择器
            content_selectors = [
                "//div[@class='entry-content']//p/text()",
                "//div[@class='field-content']//p/text()",
                "//article//p/text()",
                "//div[@class='content']//p/text()",
                "//main//p/text()",
                "//div[@class='article-content']//p/text()",
                "//div[@class='body-content']//p/text()",
                "//div[@class='content-body']//p/text()",
                "//div[@class='article-body']//p/text()",
                "//div[@class='news-content']//p/text()",
                "//div[@class='story-content']//p/text()",
                "//div[@class='entry-content']//p//text()",
                "//p/text()"
            ]
            
            content = ""
            for selector in content_selectors:
                try:
                    content_parts = response.xpath(selector).extract()
                    if content_parts:
                        # 清理每个段落并连接
                        clean_parts = []
                        for part in content_parts:
                            clean_part = re.sub(r'<[^>]+>', '', part).strip()
                            clean_part = re.sub(r'\s+', ' ', clean_part)
                            # 过滤掉广告内容
                            if clean_part and 'Publicidad' not in clean_part and 'publicidad' not in clean_part:
                                clean_parts.append(clean_part)
                        content = " ".join(clean_parts)
                        if content:
                            break
                except Exception as e:
                    print(f"提取内容时出错: {str(e)}")
            
            # 如果仍未提取到内容，尝试使用更广泛的选择器
            if not content:
                # 尝试提取所有文本内容
                all_text = response.xpath('//body//text()').extract()
                clean_text = []
                for text in all_text:
                    clean = re.sub(r'\s+', ' ', text).strip()
                    if clean and len(clean) > 50 and 'Publicidad' not in clean and 'publicidad' not in clean:
                        clean_text.append(clean)
                content = " ".join(clean_text[:10])  # 取前10个较长的文本片段
            
            # 处理特殊字符
            content = content.replace("'", "\\'").replace('"', '\\"')
            items.content = content if content else "No content"
            
            # 尝试提取发布时间
            pubtime_selectors = [
                "//time/@datetime",
                "//span[@class='date']/text()",
                "//div[@class='date']/text()",
                "//span[@class='publish-date']/text()",
                "//div[@class='publish-date']/text()",
                "//div[@class='entry-meta']/span/text()",
                "//meta[@property='article:published_time']/@content",
                "//meta[@name='article:published_time']/@content"
            ]
            
            pubtime = ""
            for selector in pubtime_selectors:
                try:
                    extracted_pubtime = response.xpath(selector).extract_first()
                    if extracted_pubtime:
                        pubtime = extracted_pubtime.strip()
                        break
                except Exception as e:
                    print(f"提取发布时间时出错: {str(e)}")
            
            items.pubtime = pubtime if pubtime else "No pubtime"
            items.author = ""
            
            print(f"提取到的标题: {items.title}")
            print(f"提取到的内容长度: {len(items.content)}")
            print(f"提取到的发布时间: {items.pubtime}")
            print(f"提取到的URL: {items.article_url}")
            print(f"提取到的关键词: {items.keyword}")
            
            # 确保所有必要字段都有值
            if not items.title:
                items.title = "No title"
            if not items.content:
                items.content = "No content"
            if not items.article_url:
                items.article_url = "No url"
            if not items.keyword:
                items.keyword = "No keyword"
            if not items.country:
                items.country = "Ecuador"
            
            print(items)
            
            # 检查内容长度，只有内容长度足够的新闻才保存
            content_words = items.content.split()
            if items.content and items.title and len(content_words) >= 30:
                yield items
            else:
                print(f"内容太短或缺少必要字段，跳过保存: {items.title}")
        except Exception as e:
            print(f"解析详情页出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return None


if __name__ == "__main__":
    AirSpiderDemo().start()
