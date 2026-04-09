# 付费
import json
import re
import time
import uuid
import random
import os

import requests
import feapder
from NewsItems import SpiderDataItem
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
        response = requests.request(
            request.method, request.url, **kwargs
        )
        response = Response(response)
        return response


class AirSpiderDemo(feapder.AirSpider):
    # 数据库连接和表结构创建
    db = MysqlDB(
        ip="192.168.42.183", port=3306, db="spider_data", user_name="czm", user_pass="root"
    )
    mysql_db = "spider_data"
    table = "Democratic_Republic_of_the_Congo"
    
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

        SPIDER_THREAD_COUNT=1,  # 爬虫并发数，设为 1 以减少被封禁的风险
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[15, 20],  # 增加请求间隔，减少被封禁的风险
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

    country = 'Democratic Republic of the Congo'
    table = 'Democratic_Republic_of_the_Congo'
    
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
    
    # 法语关键词，每个单词作为一个关键词
    keywords = ["Extrême", "Chaleur", "Haute", "Température", "Fortes", "Pluies", "Sécheresse", "Panne", "d'", "électricité", "due", "à", "la", "chaleur", "Incendie", "Pollution", "de", "l'", "air", "Changement", "climatique", "Réduction", "des", "rendements", "agricoles", "Hypoxie", "Coup", "de", "chaleur", "Impact", "de", "la", "chaleur", "sur", "le", "trafic", "Désastre", "écologique", "Impact", "du", "changement", "climatique", "sur", "l'", "économie", "Vague", "de", "chaleur", "marine", "Pollution", "liée", "à", "la", "chaleur", "Corail"]

    def start_requests(self):
        # 对每个关键词执行搜索
        for keyword in self.keywords:
            # 随机延迟
            time.sleep(random.uniform(1, 3))
            
            # 从环境变量获取代理设置
            proxy = os.environ.get('http_proxy', 'http://127.0.0.1:7897')
            proxies = {
                'http': proxy,
                'https': os.environ.get('https_proxy', proxy)
            }
            
            # 打印调试信息
            print(f"Using proxy: {proxy}")
            print(f"Proxy settings: {proxies}")
            
            # 使用搜索URL，按关键词搜索
            url = "https://actualite.cd/index.php/search/node"
            
            # 随机选择User-Agent
            user_agent = random.choice(self.user_agents)
            
            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                "priority": "u=0, i",
                "referer": "https://actualite.cd/",
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
            
            params = {
                "keys": keyword,
                "page": "0"
            }
            
            # 动态生成cookies
            cookies = {
                "_ga": f"GA1.1.{random.randint(1000000000, 9999999999)}.{int(time.time())}",
                "_fbp": f"fb.1.{int(time.time())}.{random.randint(100000000000000000, 999999999999999999)}",
                "compass_uid": f"{uuid.uuid4()}"
            }
            
            # 设置请求参数，明确指定代理
            requests_kwargs = {
                'params': params,
                'headers': headers,
                'cookies': cookies,
                'timeout': 22,
                'verify': False,
                'proxies': proxies
            }
            
            print(f"搜索关键词: {keyword}")
            yield feapder.Request(url, callback=self.parse_search_results, page=0, keyword=keyword, requests_kwargs=requests_kwargs, filter_repeat=True)

    def download_midware(self, request):
        # 随机选择User-Agent
        user_agent = random.choice(self.user_agents)
        
        # 随机延迟
        time.sleep(random.uniform(1, 3))
        
        # 完全重写请求头
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://actualite.cd/",
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
        
        # 从环境变量获取代理设置
        proxy = os.environ.get('http_proxy', 'http://127.0.0.1:7897')
        proxies = {
            'http': proxy,
            'https': os.environ.get('https_proxy', proxy)
        }
        
        # 打印调试信息
        print(f"Using proxy: {proxy}")
        print(f"Proxy settings: {proxies}")
        
        # 动态生成cookies
        cookies = {
            "_ga": f"GA1.1.{random.randint(1000000000, 9999999999)}.{int(time.time())}",
            "_fbp": f"fb.1.{int(time.time())}.{random.randint(100000000000000000, 999999999999999999)}",
            "compass_uid": f"{uuid.uuid4()}"
        }
        
        # 设置请求参数
        if not hasattr(request, 'requests_kwargs'):
            request.requests_kwargs = {}
        
        # 设置请求参数，明确指定代理
        request.requests_kwargs = {
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

    def parse_search_results(self, request, response):
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
            
            # 使用XPath选择器从搜索结果页面提取新闻链接
            links = []
            
            # 尝试多种XPath选择器
            xpath_selectors = [
                "//div[@id='det-titre']/a/@href",
                "//h2/a/@href",
                "//h3/a/@href",
                "//article/a/@href",
                "//div[@class='result-item']/a/@href",
                "//a[contains(@href, '/node/')]/@href",
                "//a[contains(@class, 'link')]/@href",
                "//div[@class='field-content']/a/@href",
                "//div[@class='node-title']/a/@href",
                "//div[@class='title']/a/@href",
                "//div[@class='teaser']/a/@href"
            ]
            
            for selector in xpath_selectors:
                try:
                    extracted_links = response.xpath(selector).extract()
                    links.extend(extracted_links)
                except Exception as e:
                    print(f"使用选择器 {selector} 提取链接时出错: {str(e)}")
            
            # 过滤重复链接
            links = list(set(links))
            
            # 确保链接是完整的URL
            valid_links = []
            for link in links:
                if link.startswith('http'):
                    # 只保留来自 actualite.cd 的链接
                    if 'actualite.cd' in link:
                        valid_links.append(link)
                elif link.startswith('/'):
                    full_url = 'https://actualite.cd' + link
                    valid_links.append(full_url)
            
            links = valid_links
            print(f"关键词 {current_keyword} 第 {current_page} 页获取到 {len(links)} 个有效结果")
            
            # 打印前几个链接以便调试
            if links:
                print(f"前3个链接: {links[:3]}")
            
            # 检查是否与上一页链接相同（用于分页结束判断）
            if current_keyword in self.previous_links and links == self.previous_links[current_keyword]:
                print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，停止爬取")
                return None
            
            self.previous_links[current_keyword] = links
            
            if not links:
                print(f"关键词 {current_keyword} 的第 {current_page} 页没有数据")
                return None
                
        except Exception as e:
            print(f"关键词 {current_keyword} 解析出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

        # 为每个链接创建请求，使用当前关键词（确保相关性）
        for item in links:
            items = SpiderDataItem()
            items.table_name = self.table
            items.article_url = item
            items.country = self.country
            items.keyword = current_keyword  # 使用当前搜索的关键词，确保相关性
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)
        
        # 继续爬取下一页
        next_page = current_page + 1
        
        # 从环境变量获取代理设置
        proxy = os.environ.get('http_proxy', 'http://127.0.0.1:7897')
        proxies = {
            'http': proxy,
            'https': os.environ.get('https_proxy', proxy)
        }
        
        # 随机选择User-Agent
        user_agent = random.choice(self.user_agents)
        
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": f"https://actualite.cd/index.php/search/node?keys={current_keyword}",
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
        
        params = {
            "keys": current_keyword,
            "page": str(next_page)
        }
        
        # 动态生成cookies
        cookies = {
            "_ga": f"GA1.1.{random.randint(1000000000, 9999999999)}.{int(time.time())}",
            "_fbp": f"fb.1.{int(time.time())}.{random.randint(100000000000000000, 999999999999999999)}",
            "compass_uid": f"{uuid.uuid4()}"
        }
        
        # 设置请求参数，明确指定代理
        requests_kwargs = {
            'params': params,
            'headers': headers,
            'cookies': cookies,
            'timeout': 22,
            'verify': False,
            'proxies': proxies
        }
        
        print(f"关键词 {current_keyword} 继续爬取第 {next_page} 页")
        yield feapder.Request("https://actualite.cd/index.php/search/node", callback=self.parse_search_results, page=next_page, keyword=current_keyword, requests_kwargs=requests_kwargs, filter_repeat=True)

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
                "//article//h1/text()"
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
                            if clean_part and 'Publicidad' not in clean_part:
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
                    if clean and len(clean) > 50:
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
                "//meta[@property='og:updated_time']/@content",
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
                items.country = "Democratic Republic of the Congo"
            
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


if __name__ == '__main__':
    AirSpiderDemo().start()
