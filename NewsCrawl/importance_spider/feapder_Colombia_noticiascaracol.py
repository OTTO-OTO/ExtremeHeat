# 付费
import json
import re
import time
import uuid
import random
import os
import hashlib

import requests
import feapder
from NewsItems import SpiderDataItem
from feapder.db.mysqldb import MysqlDB
from feapder.network.downloader._requests import RequestsDownloader
from feapder.network.response import Response

# 全局会话管理，用于保持 cookies
class SessionManager:
    def __init__(self):
        self.session = requests.Session()
        self.session.verify = False
        self.session.timeout = 22
        
    def get_session(self):
        return self.session

# 创建全局会话管理器
session_manager = SessionManager()

# 生成浏览器指纹
def generate_browser_fingerprint():
    """生成模拟浏览器指纹"""
    canvas_hash = hashlib.md5(str(random.random()).encode()).hexdigest()
    webgl_hash = hashlib.md5(str(random.random()).encode()).hexdigest()
    return {
        'canvas_hash': canvas_hash,
        'webgl_hash': webgl_hash,
        'user_agent_hash': hashlib.md5(str(random.random()).encode()).hexdigest()
    }

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
        
        # 使用全局会话
        session = session_manager.get_session()
        session.proxies = kwargs.get('proxies', {})
        
        # 发送请求
        response = session.request(
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
    table = "Colombia"
    
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

    country = 'Colombia'
    table = 'Colombia'
    
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
    
    # 西班牙语关键词，每个单词作为一个关键词
    keywords = ["Extremo", "Calor", "Alta", "Temperatura", "Lluvia", "Pesada", "Sequía", "Poder", "Corte", "de", "Electricidad", "debido", "al", "calor", "Incendio", "Contaminación", "del", "aire", "Cambio", "climático", "Reducción", "de", "los", "rendimientos", "agrícolas", "Hipoxia", "Ataque", "de", "calor", "Impacto", "del", "calor", "en", "el", "tráfico", "Desastre", "ecológico", "Impacto", "del", "cambio", "climático", "en", "la", "economía", "Ola", "de", "calor", "marina", "Contaminación", "relacionada", "con", "el", "calor", "Coral"]

    def start_requests(self):
        # 首先访问主页获取正常的 cookies
        self._initialize_session()
        
        for keyword in self.keywords:
            # 随机延迟，增加随机性
            time.sleep(random.uniform(2, 5))
            
            # 从环境变量获取代理设置
            proxy = os.environ.get('http_proxy', 'http://127.0.0.1:7897')
            proxies = {
                'http': proxy,
                'https': os.environ.get('https_proxy', proxy)
            }
            
            # 打印调试信息
            print(f"Using proxy: {proxy}")
            print(f"Proxy settings: {proxies}")
            
            page = 1
            url = "https://www.noticiascaracol.com/busqueda"
            
            # 随机选择User-Agent
            user_agent = random.choice(self.user_agents)
            
            # 生成浏览器指纹
            fingerprint = generate_browser_fingerprint()
            
            # 构建更全面的请求头
            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                "accept-encoding": "gzip, deflate, br, zstd",
                "priority": "u=0, i",
                "referer": f"https://www.noticiascaracol.com/busqueda?q={keyword}",
                "sec-ch-ua": f"\"Microsoft Edge\";v=\"{random.randint(130, 135)}\", \"Chromium\";v=\"{random.randint(130, 135)}\", \"Not_A Brand\";v=\"24\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-ch-ua-platform-version": f"\"{random.randint(10, 11)}.0.0\"",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "same-origin",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": user_agent,
                "x-forwarded-for": f"{random.randint(10, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}",
                "x-requested-with": "XMLHttpRequest",
                "dnt": "1",
                "cache-control": "max-age=0",
                "pragma": "no-cache"
            }
            
            params = {
                "q": f"{keyword}",
                "p": f"{page}"
            }
            
            # 动态生成更真实的cookies
            cookies = {
                "_ga": f"GA1.1.{random.randint(1000000000, 9999999999)}.{int(time.time())}",
                "_fbp": f"fb.1.{int(time.time())}.{random.randint(100000000000000000, 999999999999999999)}",
                "_scor_uid": f"{uuid.uuid4().hex}",
                "compass_uid": f"{uuid.uuid4()}",
                "_gid": f"GA1.2.{random.randint(1000000000, 9999999999)}.{int(time.time())}",
                "_gat_UA-12345678-1": f"{random.randint(1, 10)}",
                "_gat": f"{random.randint(1, 10)}",
                "__cfduid": f"{uuid.uuid4().hex}",
                "_ajs_user_id": f"{uuid.uuid4()}",
                "_ajs_group_id": f"{uuid.uuid4()}",
                "_ajs_session_id": f"{uuid.uuid4()}"
            }
            
            # 设置请求参数，明确指定代理
            requests_kwargs = {
                'params': params,
                'headers': headers,
                'cookies': cookies,
                'timeout': 22,
                'verify': False,
                'proxies': proxies,
                'allow_redirects': True
            }
            
            print(f"搜索关键词 {keyword}，URL: {url}")
            yield feapder.Request(url, callback=self.parse_url, page=page, keyword=keyword, requests_kwargs=requests_kwargs, filter_repeat=True)
    
    def _initialize_session(self):
        """初始化会话，访问主页获取正常的 cookies"""
        try:
            main_url = "https://www.noticiascaracol.com/"
            print(f"初始化会话，访问主页: {main_url}")
            
            # 随机 User-Agent
            user_agent = random.choice(self.user_agents)
            
            # 从环境变量获取代理设置
            proxy = os.environ.get('http_proxy', 'http://127.0.0.1:7897')
            proxies = {
                'http': proxy,
                'https': os.environ.get('https_proxy', proxy)
            }
            
            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                "accept-encoding": "gzip, deflate, br, zstd",
                "priority": "u=0, i",
                "sec-ch-ua": f"\"Microsoft Edge\";v=\"{random.randint(130, 135)}\", \"Chromium\";v=\"{random.randint(130, 135)}\", \"Not_A Brand\";v=\"24\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "none",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": user_agent
            }
            
            # 发送请求到主页
            session = session_manager.get_session()
            session.proxies = proxies
            main_response = session.get(main_url, headers=headers, timeout=22, verify=False)
            print(f"主页响应状态码: {main_response.status_code}")
            
            if main_response.status_code == 200:
                print(f"会话初始化成功，获取到 cookies: {list(session.cookies.get_dict().keys())}")
            else:
                print(f"会话初始化失败，状态码: {main_response.status_code}")
                
        except Exception as e:
            print(f"初始化会话出错: {str(e)}")

    def download_midware(self, request):
        # 随机选择User-Agent
        user_agent = random.choice(self.user_agents)
        
        # 随机延迟，增加随机性
        time.sleep(random.uniform(2, 5))
        
        # 生成浏览器指纹
        fingerprint = generate_browser_fingerprint()
        
        # 构建更全面的请求头
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "accept-encoding": "gzip, deflate, br, zstd",
            "priority": "u=0, i",
            "referer": "https://www.noticiascaracol.com/",
            "sec-ch-ua": f"\"Microsoft Edge\";v=\"{random.randint(130, 135)}\", \"Chromium\";v=\"{random.randint(130, 135)}\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-ch-ua-platform-version": f"\"{random.randint(10, 11)}.0.0\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": user_agent,
            "x-forwarded-for": f"{random.randint(10, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}",
            "x-requested-with": "XMLHttpRequest",
            "dnt": "1",
            "cache-control": "max-age=0",
            "pragma": "no-cache",
            "x-client-data": f"{fingerprint['user_agent_hash']}"
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
        
        # 动态生成更真实的cookies
        cookies = {
            "_ga": f"GA1.1.{random.randint(1000000000, 9999999999)}.{int(time.time())}",
            "_fbp": f"fb.1.{int(time.time())}.{random.randint(100000000000000000, 999999999999999999)}",
            "_scor_uid": f"{uuid.uuid4().hex}",
            "compass_uid": f"{uuid.uuid4()}",
            "_gid": f"GA1.2.{random.randint(1000000000, 9999999999)}.{int(time.time())}",
            "_gat_UA-12345678-1": f"{random.randint(1, 10)}",
            "_gat": f"{random.randint(1, 10)}",
            "__cfduid": f"{uuid.uuid4().hex}",
            "_ajs_user_id": f"{uuid.uuid4()}",
            "_ajs_group_id": f"{uuid.uuid4()}",
            "_ajs_session_id": f"{uuid.uuid4()}"
        }
        
        # 获取全局会话的cookies并合并
        session = session_manager.get_session()
        session_cookies = session.cookies.get_dict()
        cookies.update(session_cookies)
        
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
            'allow_redirects': True
        }
        
        # 确保requests库使用指定的代理和会话
        session.proxies = proxies
        
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        
        try:
            # 检查响应状态码
            print(f"响应状态码: {response.status_code}")
            
            # 打印响应头信息
            headers_dict = dict(response.headers)
            print(f"响应头: {list(headers_dict.items())[:5]}")  # 只打印前5个头部
            
            # 检查响应内容是否为空或包含重定向信息
            if not response.text or len(response.text) < 100:
                print(f"响应内容过短，可能是重定向或错误")
                print(f"响应内容: {response.text}")
                # 尝试使用备用策略
                if not self._retry_with_alternative_strategy(request):
                    return None
            
            # 检查是否是JavaScript重定向或动态加载
            if 'window.location' in response.text or 'redirect' in response.text.lower():
                print(f"检测到JavaScript重定向")
                # 尝试处理重定向
                redirect_url = self._extract_redirect_url(response.text)
                if redirect_url:
                    print(f"提取到重定向URL: {redirect_url}")
                    # 重新发起请求
                    yield feapder.Request(redirect_url, callback=self.parse_url, page=request.page, keyword=current_keyword, requests_kwargs=request.requests_kwargs, filter_repeat=True)
                    return None
            
            # 检查是否遇到 AWS WAF 验证码
            if 'awsWafCookieDomainList' in response.text or 'AwsWafIntegration' in response.text or 'WAF' in response.text or response.status_code == 202:
                print(f"检测到 AWS WAF 验证码")
                print(f"网站需要验证码验证，尝试使用高级反爬策略")
                
                # 尝试高级反爬策略
                if not self._handle_waf_challenge(request, response):
                    return None
            
            if response.status_code == 403:
                print(f"关键词 {current_keyword} 遇到403错误，尝试使用不同的反爬策略")
                # 尝试使用不同的IP和User-Agent
                if not self._retry_with_new_identity(request):
                    return None
            
            # 尝试使用多个XPath选择器提取链接
            link_selectors = [
                "//ul[@class='SearchResultsModule-results']//h2[@class='PromoB-title']/div/a/@href",
                "//div[@class='SearchResultsModule-results']//h2/a/@href",
                "//h2[@class='PromoB-title']/a/@href",
                "//a[@class='PromoB-title-link']/@href",
                "//a[contains(@href, '/noticia/')]/@href",
                "//a[contains(@href, '/colombia/')]/@href",
                "//a[contains(@href, '/internacional/')]/@href",
                "//a[contains(@class, 'link') or contains(@class, 'url')]/@href",
                "//article//a/@href",
                "//div[contains(@class, 'result')]//a/@href",
                "//div[contains(@class, 'article')]//a/@href"
            ]
            
            links = []
            for selector in link_selectors:
                try:
                    extracted_links = response.xpath(selector).extract()
                    if extracted_links:
                        print(f"使用选择器 {selector} 提取到 {len(extracted_links)} 个链接")
                        links.extend(extracted_links)
                except Exception as e:
                    print(f"使用选择器 {selector} 提取链接时出错: {str(e)}")
            
            # 过滤重复链接
            links = list(set(links))
            print(f"去重后共有 {len(links)} 个链接")
            
            # 确保链接是完整的URL并过滤出新闻页面
            valid_links = []
            for link in links:
                if link.startswith('http'):
                    url = link
                elif link.startswith('/'):
                    url = 'https://www.noticiascaracol.com' + link
                else:
                    continue
                
                # 过滤掉非新闻页面
                if any(keyword in url for keyword in ['/noticia/', '/colombia/', '/internacional/', '/nacional/', '/mundo/', '/politica/', '/economia/']):
                    # 过滤掉直播页面和分类页面
                    if not any(skip in url for skip in ['/live/', '/en-vivo/', '/categoria/', '/tags/', '/busqueda/']):
                        valid_links.append(url)
            
            # 去重
            valid_links = list(set(valid_links))
            
            links = valid_links
            print(f"过滤后获取到 {len(links)} 个有效新闻链接")
            
            # 打印前几个链接以便调试
            if links:
                print(f"前3个链接: {links[:3]}")
            
            current_page = request.page

            current_links = links
            if current_keyword in self.previous_links and current_links == self.previous_links[current_keyword]:
                print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
                return None  # 如果链接相同，返回 None 表示结束当前关键词的处理

            self.previous_links[current_keyword] = current_links  # 更新当前关键词的链接列表

            if not links:
                print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
                return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        except Exception as e:
            print(f"关键词 {current_keyword} 解析出错: {str(e)}")
            # 打印响应内容以便调试
            print(f"响应内容: {response.text[:2000]}")
            import traceback
            traceback.print_exc()
            # 尝试使用备用策略
            if not self._retry_with_alternative_strategy(request):
                return None

        for item in links:
            items = SpiderDataItem()
            items.table_name = self.table
            items.article_url = item
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.noticiascaracol.com/busqueda"
        
        # 从环境变量获取代理设置
        proxy = os.environ.get('http_proxy', 'http://127.0.0.1:7897')
        proxies = {
            'http': proxy,
            'https': os.environ.get('https_proxy', proxy)
        }
        
        # 随机选择User-Agent
        user_agent = random.choice(self.user_agents)
        
        # 生成浏览器指纹
        fingerprint = generate_browser_fingerprint()
        
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "accept-encoding": "gzip, deflate, br, zstd",
            "priority": "u=0, i",
            "referer": f"https://www.noticiascaracol.com/busqueda?q={current_keyword}",
            "sec-ch-ua": f"\"Microsoft Edge\";v=\"{random.randint(130, 135)}\", \"Chromium\";v=\"{random.randint(130, 135)}\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-ch-ua-platform-version": f"\"{random.randint(10, 11)}.0.0\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": user_agent,
            "x-forwarded-for": f"{random.randint(10, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}",
            "x-requested-with": "XMLHttpRequest",
            "dnt": "1",
            "cache-control": "max-age=0",
            "pragma": "no-cache",
            "x-client-data": f"{fingerprint['user_agent_hash']}"
        }
        
        params = {
            "q": f"{current_keyword}",
            "p": f"{current_page}"
        }
        
        # 动态生成更真实的cookies
        cookies = {
            "_ga": f"GA1.1.{random.randint(1000000000, 9999999999)}.{int(time.time())}",
            "_fbp": f"fb.1.{int(time.time())}.{random.randint(100000000000000000, 999999999999999999)}",
            "_scor_uid": f"{uuid.uuid4().hex}",
            "compass_uid": f"{uuid.uuid4()}",
            "_gid": f"GA1.2.{random.randint(1000000000, 9999999999)}.{int(time.time())}",
            "_gat_UA-12345678-1": f"{random.randint(1, 10)}",
            "_gat": f"{random.randint(1, 10)}",
            "__cfduid": f"{uuid.uuid4().hex}",
            "_ajs_user_id": f"{uuid.uuid4()}",
            "_ajs_group_id": f"{uuid.uuid4()}",
            "_ajs_session_id": f"{uuid.uuid4()}"
        }
        
        # 获取全局会话的cookies并合并
        session = session_manager.get_session()
        session_cookies = session.cookies.get_dict()
        cookies.update(session_cookies)
        
        # 设置请求参数，明确指定代理
        requests_kwargs = {
            'params': params,
            'headers': headers,
            'cookies': cookies,
            'timeout': 22,
            'verify': False,
            'proxies': proxies,
            'allow_redirects': True
        }
        
        # 随机延迟，增加随机性
        time.sleep(random.uniform(2, 5))
        
        yield feapder.Request(url, callback=self.parse_url, page=current_page, keyword=current_keyword, requests_kwargs=requests_kwargs, filter_repeat=True)
    
    def _retry_with_alternative_strategy(self, request):
        """使用备用策略重试请求"""
        try:
            print("尝试使用备用策略重试请求")
            
            # 随机延迟
            time.sleep(random.uniform(5, 10))
            
            # 重新初始化会话
            self._initialize_session()
            
            # 生成新的请求参数
            user_agent = random.choice(self.user_agents)
            fingerprint = generate_browser_fingerprint()
            
            # 从环境变量获取代理设置
            proxy = os.environ.get('http_proxy', 'http://127.0.0.1:7897')
            proxies = {
                'http': proxy,
                'https': os.environ.get('https_proxy', proxy)
            }
            
            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                "accept-encoding": "gzip, deflate, br, zstd",
                "priority": "u=0, i",
                "referer": "https://www.noticiascaracol.com/",
                "sec-ch-ua": f"\"Microsoft Edge\";v=\"{random.randint(130, 135)}\", \"Chromium\";v=\"{random.randint(130, 135)}\", \"Not_A Brand\";v=\"24\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-ch-ua-platform-version": f"\"{random.randint(10, 11)}.0.0\"",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "same-origin",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": user_agent,
                "x-forwarded-for": f"{random.randint(10, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}",
                "x-requested-with": "XMLHttpRequest",
                "dnt": "1",
                "cache-control": "max-age=0",
                "pragma": "no-cache",
                "x-client-data": f"{fingerprint['user_agent_hash']}"
            }
            
            # 动态生成新的cookies
            cookies = {
                "_ga": f"GA1.1.{random.randint(1000000000, 9999999999)}.{int(time.time())}",
                "_fbp": f"fb.1.{int(time.time())}.{random.randint(100000000000000000, 999999999999999999)}",
                "_scor_uid": f"{uuid.uuid4().hex}",
                "compass_uid": f"{uuid.uuid4()}"
            }
            
            # 获取全局会话的cookies并合并
            session = session_manager.get_session()
            session_cookies = session.cookies.get_dict()
            cookies.update(session_cookies)
            
            # 设置新的请求参数
            requests_kwargs = {
                'headers': headers,
                'cookies': cookies,
                'timeout': 22,
                'verify': False,
                'proxies': proxies,
                'allow_redirects': True
            }
            
            print(f"使用新的请求参数重试: {request.url}")
            yield feapder.Request(request.url, callback=self.parse_url, page=request.page, keyword=request.keyword, requests_kwargs=requests_kwargs, filter_repeat=True)
            return True
        except Exception as e:
            print(f"使用备用策略重试失败: {str(e)}")
            return False
    
    def _handle_waf_challenge(self, request, response):
        """处理 AWS WAF 验证码挑战"""
        try:
            print("处理 AWS WAF 验证码挑战")
            
            # 随机延迟
            time.sleep(random.uniform(10, 15))
            
            # 重新初始化会话
            self._initialize_session()
            
            # 使用全局会话
            session = session_manager.get_session()
            
            # 从环境变量获取代理设置
            proxy = os.environ.get('http_proxy', 'http://127.0.0.1:7897')
            proxies = {
                'http': proxy,
                'https': os.environ.get('https_proxy', proxy)
            }
            session.proxies = proxies
            
            # 生成新的请求参数
            user_agent = random.choice(self.user_agents)
            fingerprint = generate_browser_fingerprint()
            
            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                "accept-encoding": "gzip, deflate, br, zstd",
                "priority": "u=0, i",
                "referer": "https://www.noticiascaracol.com/",
                "sec-ch-ua": f"\"Microsoft Edge\";v=\"{random.randint(130, 135)}\", \"Chromium\";v=\"{random.randint(130, 135)}\", \"Not_A Brand\";v=\"24\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-ch-ua-platform-version": f"\"{random.randint(10, 11)}.0.0\"",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "same-origin",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": user_agent,
                "x-forwarded-for": f"{random.randint(10, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}",
                "x-requested-with": "XMLHttpRequest",
                "dnt": "1",
                "cache-control": "max-age=0",
                "pragma": "no-cache",
                "x-client-data": f"{fingerprint['user_agent_hash']}"
            }
            
            # 先访问主页
            main_url = "https://www.noticiascaracol.com/"
            print(f"访问主页: {main_url}")
            main_response = session.get(main_url, headers=headers, timeout=22, verify=False)
            print(f"主页响应状态码: {main_response.status_code}")
            
            if main_response.status_code == 200:
                # 再访问搜索页面
                print(f"访问搜索页面: {request.url}")
                search_response = session.get(request.url, headers=headers, timeout=22, verify=False)
                print(f"搜索页面响应状态码: {search_response.status_code}")
                
                if search_response.status_code == 200:
                    print("WAF 挑战处理成功")
                    # 使用新的响应内容进行解析
                    from lxml import etree
                    new_response = type('obj', (object,), {'text': search_response.text, 'xpath': lambda self, x: etree.HTML(self.text).xpath(x), 'status_code': search_response.status_code})()
                    # 重新解析
                    return self.parse_url(request, new_response)
                else:
                    print(f"搜索页面访问失败，状态码: {search_response.status_code}")
                    return False
            else:
                print(f"主页访问失败，状态码: {main_response.status_code}")
                return False
        except Exception as e:
            print(f"处理 WAF 挑战失败: {str(e)}")
            return False
    
    def _retry_with_new_identity(self, request):
        """使用新的身份信息重试请求"""
        try:
            print("使用新的身份信息重试请求")
            
            # 随机延迟
            time.sleep(random.uniform(5, 10))
            
            # 生成新的 IP 地址
            new_ip = f"{random.randint(10, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"
            print(f"使用新的 IP 地址: {new_ip}")
            
            # 生成新的 User-Agent
            user_agent = random.choice(self.user_agents)
            print(f"使用新的 User-Agent: {user_agent}")
            
            # 生成浏览器指纹
            fingerprint = generate_browser_fingerprint()
            
            # 从环境变量获取代理设置
            proxy = os.environ.get('http_proxy', 'http://127.0.0.1:7897')
            proxies = {
                'http': proxy,
                'https': os.environ.get('https_proxy', proxy)
            }
            
            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                "accept-encoding": "gzip, deflate, br, zstd",
                "priority": "u=0, i",
                "referer": "https://www.noticiascaracol.com/",
                "sec-ch-ua": f"\"Microsoft Edge\";v=\"{random.randint(130, 135)}\", \"Chromium\";v=\"{random.randint(130, 135)}\", \"Not_A Brand\";v=\"24\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-ch-ua-platform-version": f"\"{random.randint(10, 11)}.0.0\"",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "same-origin",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": user_agent,
                "x-forwarded-for": new_ip,
                "x-requested-with": "XMLHttpRequest",
                "dnt": "1",
                "cache-control": "max-age=0",
                "pragma": "no-cache",
                "x-client-data": f"{fingerprint['user_agent_hash']}"
            }
            
            # 动态生成新的cookies
            cookies = {
                "_ga": f"GA1.1.{random.randint(1000000000, 9999999999)}.{int(time.time())}",
                "_fbp": f"fb.1.{int(time.time())}.{random.randint(100000000000000000, 999999999999999999)}",
                "_scor_uid": f"{uuid.uuid4().hex}",
                "compass_uid": f"{uuid.uuid4()}"
            }
            
            # 设置新的请求参数
            requests_kwargs = {
                'headers': headers,
                'cookies': cookies,
                'timeout': 22,
                'verify': False,
                'proxies': proxies,
                'allow_redirects': True
            }
            
            print(f"使用新的身份信息重试: {request.url}")
            yield feapder.Request(request.url, callback=self.parse_url, page=request.page, keyword=request.keyword, requests_kwargs=requests_kwargs, filter_repeat=True)
            return True
        except Exception as e:
            print(f"使用新的身份信息重试失败: {str(e)}")
            return False
    
    def _extract_redirect_url(self, html):
        """从 HTML 中提取重定向 URL"""
        try:
            # 提取 window.location.href
            match = re.search(r'window\.location\.href\s*=\s*["\'](.*?)["\']', html)
            if match:
                return match.group(1)
            # 提取 window.location
            match = re.search(r'window\.location\s*=\s*["\'](.*?)["\']', html)
            if match:
                return match.group(1)
            # 提取 meta 标签重定向
            match = re.search(r'<meta\s+http-equiv=["\']refresh["\']\s+content=["\']\d+;\s*url=(.*?)["\']', html)
            if match:
                return match.group(1)
            return None
        except Exception as e:
            print(f"提取重定向 URL 失败: {str(e)}")
            return None

    def parse_detail(self, request, response):
        try:
            items = request.items
            items.table_name = self.table
            
            # 检查响应状态码
            if hasattr(response, 'status_code') and response.status_code == 202:
                print(f"URL {items.article_url} 返回 202 状态码，可能被反爬")
                # 尝试使用备用策略
                if not self._retry_with_alternative_strategy(request):
                    return None
            
            # 检查响应内容是否为空
            if not hasattr(response, 'text') or not response.text:
                print(f"URL {items.article_url} 响应内容为空")
                return None
            
            # 检查响应内容长度
            if len(response.text) < 1000:
                print(f"URL {items.article_url} 响应内容过短，可能被反爬")
                # 尝试使用备用策略
                if not self._retry_with_alternative_strategy(request):
                    return None
            
            # 尝试提取标题 - 使用测试中发现的正确选择器
            title_selectors = [
                "//h1/text()",
                "//h1[@class='ArticlePage-headline']/text()",
                "//h1[@class='Headline-headline']/text()",
                "//h1[@class='title']/text()",
                "//h1[@class='article-title']/text()",
                "//div[@class='headline']/h1/text()",
                "//header//h1/text()",
                "//article//h1/text()",
                "//title/text()"
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
                "//div[@class='ArticlePage-content']//p//text()",
                "//div[@class='ArticlePage-body']//p//text()",
                "//div[@class='article-content']//p//text()",
                "//div[@class='content']//p//text()",
                "//main//p//text()",
                "//article//p//text()",
                "//div[@class='body-content']//p//text()",
                "//div[@class='content-body']//p//text()",
                "//div[@class='article-body']//p//text()",
                "//div[@class='news-content']//p//text()",
                "//div[@class='story-content']//p//text()",
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
                items.country = "Colombia"
            
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
            # 尝试使用备用策略
            if not self._retry_with_alternative_strategy(request):
                return None

if __name__ == "__main__":
    AirSpiderDemo().start()
