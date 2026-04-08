# -*- coding: utf-8 -*-

import feapder
from feapder import Item
from lxml import etree
from feapder.db.mysqldb import MysqlDB
import random
import time
import string
import hashlib
from playwright.sync_api import sync_playwright


class FranceLemondeSpider(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.42.183", port=3306, db="spider_data", user_name="czm", user_pass="root"
    )
    # 创建表结构
    table = 'France_2'
    create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS `spider_data`.`{table}`  (
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

        SPIDER_THREAD_COUNT=3,  # 降低并发数，减少被反爬的风险
        SPIDER_SLEEP_TIME=[10, 15],  # 增加请求间隔
        SPIDER_MAX_RETRY_TIMES=3,  # 增加重试次数
        MYSQL_IP="192.168.42.183",
        MYSQL_PORT=3306,
        MYSQL_DB="spider_data",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        MYSQL_AUTO_CREATE_TABLE=True,
        # 数据库连接池配置
        MYSQL_POOL_SIZE=10,  # 连接池大小
        MYSQL_POOL_PRE_PING=True,  # 连接池预 ping
        MYSQL_POOL_RECYCLE=3600,  # 连接回收时间
        MYSQL_CONNECT_TIMEOUT=10,  # 连接超时时间
        ITEM_FILTER_ENABLE=False,  # 禁用 item 去重，使用数据库唯一索引确保不重复
    )

    country = 'France'
    # 英文和法文关键词
    keywords = [ 
        "Extreme", "Extrême", 
        "Heat", "Chaleur", 
        "High", "Haute", 
        "Temperature", "Température", 
        "Heavy", "Fortes", 
        "Rain", "Pluies", 
        "Drought", "Sécheresse", 
        "Power", "Électricité", 
        "Outage", "Panne", 
        "Fire", "Incendie", 
        "Air", "Air", 
        "Pollution", "Pollution", 
        "Climate", "Climatique", 
        "Change", "Changement", 
        "Crop", "Rendements", 
        "Yield", "Agricoles", 
        "Reduction", "Réduction", 
        "Oxygen", "Oxygène", 
        "Deficiency", "Hypoxie", 
        "Affecting", "Affectant", 
        "Traffic", "Circulation", 
        "Ecological", "Écologique", 
        "Disaster", "Catastrophe", 
        "Economy", "Économie", 
        "Marine", "Marine", 
        "Heatwave", "Chaleur", 
        "Coral", "Corail",
        "stroke", "chaleur",
        "de"
    ]
    
    # 随机用户代理列表
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/122.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/131.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0"
    ]
    
    def __init__(self):
        super().__init__()
        self.previous_links = {}  # 使用字典存储每个关键词的上一页链接
        self.session_id = self.generate_session_id()

    def generate_session_id(self):
        """生成随机会话ID"""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=32))

    def start_requests(self):
        import datetime
        # 获取当前日期作为结束日期
        end_date = datetime.datetime.now().strftime("%d/%m/%Y")
        
        for keyword in self.keywords:
            # 随机延迟，避免请求过于集中
            time.sleep(random.uniform(1, 3))
            page = 1
            url = "https://www.lemonde.fr/recherche/"
            params = {
                "search_keywords": f"{keyword}",
                "start_at": "19/12/1944",
                "end_at": end_date,
                "search_sort": "relevance_desc",
                "page": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        # 禁用系统代理，直接连接
        import os
        import datetime
        # 清除环境变量中的代理设置
        for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy']:
            if key in os.environ:
                del os.environ[key]
        
        # 随机选择用户代理
        user_agent = random.choice(self.user_agents)
        
        # 动态生成referer
        referer_keyword = random.choice(self.keywords)
        end_date = datetime.datetime.now().strftime('%d/%m/%Y')
        # 使用字符串连接避免f-string中的百分号转义问题
        referer = "https://www.lemonde.fr/recherche/?search_keywords=" + referer_keyword.replace(' ', '+') + "&start_at=19/12/1944&end_at=" + end_date + "&search_sort=relevance_desc"
        
        # 设置请求头
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": referer,
            "sec-ch-ua": f"\"Microsoft Edge\";v=\"{random.randint(120, 131)}\", \"Chromium\";v=\"{random.randint(120, 131)}\", \"Not_A Brand\";v=\"24\"",
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
        request.cookies = {
            "_pctx": "%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAE0RXSwH18yBbANYAHAGwBzAByUAnAB9%2BAVgAMMejADGMkAF8gA",
            "lmd_pa": str(int(time.time())),
            "atidx": self.generate_session_id(),
            "atid": self.generate_session_id(),
            "euconsent-v2": "CQKFpAAQKFpAAFzABBFRBTFsAP_gAEPgAAqII7MB7C7OQSFicX51AOsESY1ewkBSoEAgBAABgQgBABgAsIwRkGACIAHAAqAKAAIAImRBIQAtGAjABAAAQIAAASCEAECAABAAJKAAAEAQAAEACAgACQEAAAAAgEAAAAQAgAJAAEooQERAAgAgLAAAIAABAIAEAgAIAAAAAAAAAAAAAGAAAAAAAAAAAAAAAAAAEEBAARBQiIACwICQggDCCBACoIQAIAAAQAAAAwAAAAAwAUAYACDABAAAAAAAEAAAAARAAgAAAgAQgAAAAIEAAAAAAAAAAAAgAABAAAAAAAAAAAAQAAIAAAAAAAAAAAAIAQAAgAAAAICAAAAAQFgAAAAAAgEAAAAAAAAAAAAAAAAAAACAAA",
            "lmd_consent": "%7B%22userId%22%3A%22" + self.generate_session_id() + "%22%2C%22timestamp%22%3A%22" + str(time.time()) + ".169987654%22%2C%22version%22%3A1%2C%22cmpId%22%3A371%2C%22displayMode%22%3A%22cookiewall%22%2C%22purposes%22%3A%7B%22analytics%22%3Atrue%2C%22ads%22%3Atrue%2C%22personalization%22%3Atrue%2C%22mediaPlatforms%22%3Atrue%2C%22social%22%3Atrue%7D%7D",
            "pa_privacy": "%22optin%22",
            "atauthority": "%7B%22name%22%3A%22atauthority%22%2C%22val%22%3A%7B%22authority_name%22%3A%22default%22%2C%22visitor_mode%22%3A%22optin%22%7D%2C%22options%22%3A%7B%22end%22%3A%222026-12-31T23%3A59%3A59.586Z%22%2C%22path%22%3A%22%2F%22%7D%7D",
            "_cb": ''.join(random.choices(string.ascii_letters, k=16)),
            "_gcl_au": "1.1." + str(random.randint(1000000000, 9999999999)) + "." + str(int(time.time())),
            "_fbp": "fb.1." + str(int(time.time())) + "." + str(random.randint(100000000000000000, 999999999999999999)),
            "__spdt": self.generate_session_id(),
            "lead": self.generate_session_id(),
            "lead_ads": self.generate_session_id(),
            "_pin_unauth": "dWlkPU9XSm1Zek15TmpZdE5UVXlaaTAwTTJFMkxXRmhNakV0TVdGa056SXhOV1l4T1dGbQ",
            "atidvisitor": "%7B%22name%22%3A%22atidvisitor%22%2C%22val%22%3A%7B%22vrn%22%3A%22-" + str(random.randint(10000, 99999)) + "-%22%7D%2C%22options%22%3A%7B%22path%22%3A%22%2F%22%2C%22session%22%3A15724800%2C%22end%22%3A15724800%7D%7D",
            "_cs_c": "1",
            "lmd_cap": "_" + ''.join(random.choices(string.ascii_lowercase + string.digits, k=10)),
            "cto_bundle": ''.join(random.choices(string.ascii_letters + string.digits + "-_", k=100)),
            "_cs_cvars": "%7B%222%22%3A%5B%22user_status%22%2C%22gratuit%22%5D%7D",
            "_cs_id": self.generate_session_id() + "." + str(int(time.time())) + ".3." + str(int(time.time())) + "." + str(int(time.time())) + "." + str(int(time.time())) + ".1769430569199.1",
            "kw.session_ts": str(int(time.time())),
            "_sp_ses.fb3f": "*",
            "lmd_ab": ''.join(random.choices(string.ascii_letters + string.digits + "%2F%2B%3D", k=150)),
            "lmd_edg": ''.join(random.choices(string.ascii_letters + string.digits + "%2F%2B%3D", k=200)),
            "_cb_svref": "external",
            "_rdt_uuid": str(int(time.time())) + "." + self.generate_session_id(),
            "_chartbeat2": "." + str(int(time.time())) + "." + str(int(time.time())) + ".0000000000000011." + ''.join(random.choices(string.ascii_letters, k=20)) + ".6",
            "amplitude_id_7dec6d6e90d5288af6e9a882f8def199lemonde.fr": "eyJkZXZpY2VJZCI6IjIyNThiYjE1LTEyNTUtNGUyNC1hZmQwLWNhNDFjOTM3ZDk3YlIiLCJ1c2VySWQiOm51bGwsIm9wdE91dCI6ZmFsc2UsInNlc3Npb25JZCI6" + str(int(time.time())) + ",bGltZSI6MTMzfQ==",
            "kw.pv_session": str(random.randint(1, 50)),
            "_sp_id.fb3f": self.generate_session_id() + "." + str(int(time.time())) + ".6." + str(int(time.time())) + "." + str(int(time.time())) + "." + self.generate_session_id(),
            "_chartbeat5": str(random.randint(100, 999)) + "|" + str(random.randint(1000, 9999)) + "|%2Frecherche%2F|https%3A%2F%2Fwww.lemonde.fr%2Frecherche%2F%3Fsearch_keywords%3DImpact%2520trafic%26start_at%3D19%2F12%2F1944%26end_at%3D23%2F01%2F2025%26search_sort%3Drelevance_desc%26page%3D2|" + ''.join(random.choices(string.ascii_letters, k=20)) + "||c|" + ''.join(random.choices(string.ascii_letters, k=20)) + "|lemonde.fr|",
        }
        
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        current_page = request.page
        print(f"当前关键词{current_keyword}的页数为:{current_page}")
        
        # 构建完整的URL
        import urllib.parse
        url = "https://www.lemonde.fr/recherche/?"
        params = {
            "search_keywords": current_keyword,
            "start_at": "19/12/1944",
            "end_at": time.strftime("%d/%m/%Y"),
            "search_sort": "relevance_desc",
            "page": str(current_page)
        }
        full_url = url + urllib.parse.urlencode(params)
        print(f"请求URL: {full_url}")
        
        # 使用Playwright抓取页面
        links = []
        try:
            with sync_playwright() as p:
                # 启动浏览器
                browser = p.chromium.launch(
                    headless=True,  # 无头模式，提高速度
                    args=[
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-blink-features=AutomationControlled"
                    ]
                )
                context = browser.new_context(
                    user_agent=random.choice(self.user_agents),
                    locale="fr-FR",
                    geolocation={"latitude": 48.8566, "longitude": 2.3522}  # 巴黎坐标
                )
                page = context.new_page()
                
                # 设置页面大小
                page.set_viewport_size({"width": 1920, "height": 1080})
                
                # 访问页面
                print(f"正在访问页面: {full_url}")
                page.goto(full_url, wait_until="domcontentloaded", timeout=60000)
                
                # 等待页面加载完成
                time.sleep(random.uniform(5, 8))
                
                # 尝试滚动页面，触发动态加载
                page.mouse.wheel(0, 1000)
                time.sleep(2)
                page.mouse.wheel(0, 1000)
                time.sleep(2)
                
                # 保存响应内容用于调试
                if current_keyword == "Coral" and current_page == 1:
                    content = page.content()
                    with open(f"lemonde_response_{current_keyword}.html", "w", encoding="utf-8") as f:
                        f.write(content)
                    print(f"已保存Coral关键词的响应到lemonde_response_Coral.html")
                    print(f"页面内容长度: {len(content)}")
                
                # 打印页面标题
                title = page.title()
                print(f"页面标题: {title}")
                
                # 打印页面URL
                print(f"当前页面URL: {page.url}")
                
                # 提取链接
                # 尝试多种选择器
                selectors = [
                    "section.js-river-search a.js-teaser__link.teaser__link",
                    "section.teaser__list a.teaser__link",
                    "a.js-teaser__link.teaser__link",
                    "a.teaser__link",
                    "article.teaser a",
                    "div.teaser__content a",
                    "h3.teaser__title a",
                    "div.teaser a",
                    "article a"
                ]
                
                # 尝试使用 XPath 选择器
                xpath_selectors = [
                    "//a[contains(@class, 'teaser__link')]",
                    "//h3[contains(@class, 'teaser__title')]/a",
                    "//article[contains(@class, 'teaser')]/a",
                    "//div[contains(@class, 'teaser__content')]/a",
                    "//a[starts-with(@href, '/') and not(contains(@href, 'recherche'))]"
                ]
                
                for selector in selectors:
                    elements = page.query_selector_all(selector)
                    if elements:
                        for element in elements:
                            href = element.get_attribute("href")
                            if href:
                                links.append(href)
                        print(f"选择器 '{selector}' 找到 {len(elements)} 个链接")
                
                # 尝试 XPath 选择器
                for selector in xpath_selectors:
                    elements = page.query_selector_all(selector)
                    if elements:
                        for element in elements:
                            href = element.get_attribute("href")
                            if href:
                                links.append(href)
                        print(f"XPath 选择器 '{selector}' 找到 {len(elements)} 个链接")
                
                # 尝试直接查找所有链接
                all_links = page.query_selector_all("a")
                print(f"找到所有链接: {len(all_links)}")
                for link in all_links[:10]:  # 只打印前10个链接
                    href = link.get_attribute("href")
                    text = link.text_content().strip()
                    if href:
                        print(f"链接: {href}, 文本: {text[:50]}...")
                        links.append(href)
                
                # 关闭浏览器
                browser.close()
                
        except Exception as e:
            print(f"Playwright抓取失败: {e}")
        
        # 去重
        links = list(set(links))
        print(f"去重后找到 {len(links)} 个链接")
        
        current_page = request.page
        current_links = links
        
        # 检查当前关键词的上一页链接
        if current_keyword in self.previous_links and current_links == self.previous_links[current_keyword]:
            print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理

        # 更新当前关键词的上一页链接
        self.previous_links[current_keyword] = current_links

        print(f"找到 {len(links)} 个链接")
        
        for item in links:
            # 确保链接是完整的URL
            if not item.startswith('http'):
                item = "https://www.lemonde.fr" + item
            
            # 过滤掉非新闻链接
            if not item.startswith("https://www.lemonde.fr/"):
                continue
            
            # 过滤掉搜索结果页和其他非新闻页面
            non_news_patterns = [
                "/recherche/", "/tag/", "/rubrique/", "/selections/", "/abonnement/",
                "/livres/", "/videos/", "/podcasts/", "/blogs/", "/magazine/"
            ]
            
            if any(pattern in item for pattern in non_news_patterns):
                continue
            
            # 只保留新闻文章链接
            if not any(news_pattern in item for news_pattern in ["/actualite/", "/politique/", "/monde/", "/societe/", "/economie/"]):
                # 检查是否是其他类型的新闻链接
                if ".html" not in item and "/" == item[-1]:
                    continue
            
            items = Item()
            items.article_url = item
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            # 随机延迟，避免请求过于集中
            time.sleep(random.uniform(1, 3))
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        # 准备下一页请求，即使当前页没有数据也要尝试
        current_page = request.page + 1
        # 移除页数限制，让爬虫一直翻页直到没有新内容
        
        import datetime
        end_date = datetime.datetime.now().strftime("%d/%m/%Y")
        
        url = "https://www.lemonde.fr/recherche/"
        params = {
            "search_keywords": f"{current_keyword}",
            "start_at": "19/12/1944",
            "end_at": end_date,
            "search_sort": "relevance_desc",
            "page": f"{current_page}"
        }
        # 随机延迟，避免请求过于集中
        time.sleep(random.uniform(2, 4))
        print(f"准备请求关键词 {current_keyword} 的第 {current_page} 页")
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        article_url = items.article_url
        
        # 初始化所有必要的属性，避免AttributeError
        items.title = ""
        items.content = ""
        items.author = ""
        items.pubtime = ""
        
        # 随机延迟，模拟人类阅读
        time.sleep(random.uniform(2, 4))
        
        # 使用Playwright抓取文章详情页
        try:
            with sync_playwright() as p:
                # 启动浏览器
                browser = p.chromium.launch(
                    headless=True,
                    args=[
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-blink-features=AutomationControlled"
                    ]
                )
                context = browser.new_context(
                    user_agent=random.choice(self.user_agents),
                    locale="fr-FR",
                    geolocation={"latitude": 48.8566, "longitude": 2.3522}
                )
                page = context.new_page()
                
                # 设置页面大小
                page.set_viewport_size({"width": 1920, "height": 1080})
                
                # 访问文章页面
                page.goto(article_url, wait_until="domcontentloaded", timeout=60000)
                
                # 等待页面加载完成
                time.sleep(random.uniform(3, 5))
                
                # 尝试滚动页面，触发动态加载
                page.mouse.wheel(0, 500)
                time.sleep(2)
                
                # 提取标题
                title_selectors = [
                    "h1.ds-title",
                    "h1.article__title",
                    "h1.article__title--xxl",
                    "h1"
                ]
                
                for selector in title_selectors:
                    element = page.query_selector(selector)
                    if element:
                        items.title = element.text_content().strip()
                        break
                
                # 提取内容
                content_selectors = [
                    "p.article__paragraph",
                    "div.article__content p",
                    "article.article__content p",
                    "article p",
                    "div.content p"
                ]
                
                content = []
                for selector in content_selectors:
                    elements = page.query_selector_all(selector)
                    if elements:
                        for element in elements:
                            content.append(element.text_content())
                
                items.content = "".join(content).strip().replace("\r", '').replace("\n", '')
                
                # 提取发布时间
                pubtime_selectors = [
                    "time",
                    "span.meta__date",
                    "span.article__date"
                ]
                
                for selector in pubtime_selectors:
                    element = page.query_selector(selector)
                    if element:
                        # 优先获取datetime属性
                        datetime_attr = element.get_attribute("datetime")
                        if datetime_attr:
                            items.pubtime = datetime_attr
                        else:
                            items.pubtime = element.text_content().strip()
                        break
                
                # 提取作者
                author_selectors = [
                    "span.meta__author",
                    "span.article__author",
                    "div.article__author",
                    "span.author"
                ]
                
                for selector in author_selectors:
                    element = page.query_selector(selector)
                    if element:
                        items.author = element.text_content().strip()
                        break
                
                # 关闭浏览器
                browser.close()
                
        except Exception as e:
            print(f"Playwright抓取文章详情失败: {e}")
        
        # 确保所有必要字段都有值
        if not items.title:
            items.title = "Untitled"
        
        if not items.content:
            items.content = "No content"
        
        if not items.author:
            items.author = ''
        
        if not items.pubtime:
            items.pubtime = ''
        
        # 打印调试信息
        print(f"标题: {items.title}")
        print(f"链接: {items.article_url}")
        print(f"发布时间: {items.pubtime}")
        print(f"内容长度: {len(items.content)}")
        
        if items.content and items.content != "No content":
            yield items


if __name__ == "__main__":
    FranceLemondeSpider().start()
