# -*- coding: utf-8 -*-

import feapder
from feapder import Item
from lxml import etree
from feapder.db.mysqldb import MysqlDB
import random
import time
import string
import hashlib


class FranceCoralSpider(feapder.AirSpider):
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

        SPIDER_THREAD_COUNT=1,  # 降低并发数，减少被反爬的风险
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
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )

    country = 'France'
    # 只测试Coral关键词
    keywords = [ 
        "Coral", "Corail"
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
        
        # 生成随机版本号
        edge_version = str(random.randint(120, 131))
        
        # 设置请求头
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": referer,
            "sec-ch-ua": '"Microsoft Edge";v="' + edge_version + '", "Chromium";v="' + edge_version + '", "Not_A Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
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
            "_chartbeat5": str(random.randint(100, 999)) + "|" + str(random.randint(1000, 9999)) + "|%2Frecherche%2F|https%3A%2F%2Fwww.lemonde.fr%2Frecherche%2F%3Fsearch_keywords%3DImpact%2520trafic%26start_at%3D19%2F12%2F1944%26end_at%3D23%2F01%2F2025%26search_sort%3Drelevance_desc%26page%3D2|" + ''.join(random.choices(string.ascii_letters, k=20)) + "||c|" + ''.join(random.choices(string.ascii_letters, k=20)) + "|lemonde.fr|"
        }
        
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        
        # 随机延迟，模拟人类操作
        time.sleep(random.uniform(2, 5))
        
        # 打印响应状态码和URL
        print(f"响应状态码: {response.status_code}")
        print(f"请求URL: {response.url}")
        print(f"响应内容长度: {len(response.text)}")
        
        # 保存响应内容用于调试
        if current_keyword == "Coral" and request.page == 1:
            # 先检查响应内容是否为空
            if response.text:
                with open(f"lemonde_response_{current_keyword}.html", "w", encoding="utf-8") as f:
                    f.write(response.text)
                print(f"已保存Coral关键词的响应到lemonde_response_Coral.html")
            else:
                print("响应内容为空，无法保存")
                # 尝试打印响应头
                print(f"响应头: {response.headers}")
        
        # 尝试多种XPath选择器来提取链接
        links = []
        
        # 第一种选择器 - 基于我们的分析
        selector1 = response.xpath("//section[@class='js-river-search']//a[@class='js-teaser__link teaser__link']/@href")
        if selector1:
            links.extend(selector1.extract())
            print(f"选择器1找到 {len(selector1)} 个链接")
        
        # 第二种选择器
        selector2 = response.xpath("//section[@class='teaser__list teaser__list--block teaser__list--friend teaser__list--search old__teaser-list']//a/@href")
        if selector2:
            links.extend(selector2.extract())
            print(f"选择器2找到 {len(selector2)} 个链接")
        
        # 第三种选择器
        selector3 = response.xpath("//section[@class='js-teaser teaser teaser--inline-picture']//a/@href")
        if selector3:
            links.extend(selector3.extract())
            print(f"选择器3找到 {len(selector3)} 个链接")
        
        # 第四种选择器
        selector4 = response.xpath("//a[@class='js-teaser__link teaser__link']/@href")
        if selector4:
            links.extend(selector4.extract())
            print(f"选择器4找到 {len(selector4)} 个链接")
        
        # 第五种选择器 - 更通用的选择器
        selector5 = response.xpath("//a[contains(@class, 'teaser__link')]/@href")
        if selector5:
            links.extend(selector5.extract())
            print(f"选择器5找到 {len(selector5)} 个链接")
        
        # 第六种选择器 - 直接查找所有可能的新闻链接
        selector6 = response.xpath("//a[starts-with(@href, '/') and not(contains(@href, 'javascript')) and not(contains(@href, '#'))]/@href")
        if selector6:
            links.extend(selector6.extract())
            print(f"选择器6找到 {len(selector6)} 个链接")
        
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
            if "/recherche/" in item or "/tag/" in item or "/rubrique/" in item:
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
        # 限制最大页数，避免无限循环
        if current_page > 10:
            print(f"关键词 {current_keyword} 已达到最大页数限制，退出当前关键字的循环")
            return None
        
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
        
        # 随机延迟，模拟人类阅读
        time.sleep(random.uniform(1, 3))
        
        # 提取标题 - 更新为我们分析的选择器
        title_selectors = [
            "//h1[@class='ds-title']/text()",
            "//h1[@class='article__title']/text()",
            "//h1[@class='article__title article__title--xxl']/text()",
            "//h1/text()",
            "//title/text()"
        ]
        
        for selector in title_selectors:
            title = response.xpath(selector).extract_first()
            if title:
                items.title = title.strip()
                break
        
        # 提取内容 - 更新为我们分析的选择器
        content_selectors = [
            "//p[@class='article__paragraph ']//text()",
            "//div[@class='article__content old__article-content-single']//p/text()",
            "//article[@class='article__content old__article-content-single']//p/text()",
            "//article//p/text()",
            "//div[@class='content']//p/text()"
        ]
        
        content = []
        for selector in content_selectors:
            content_parts = response.xpath(selector).extract()
            if content_parts:
                content.extend(content_parts)
        
        items.content = "".join(content).strip().replace("\r", '').replace("\n", '')
        
        # 提取发布时间 - 更新为我们分析的选择器
        pubtime_selectors = [
            "//time/@datetime",
            "//time/text()",
            "//span[@class='meta__date']/text()",
            "//span[@class='meta__date meta__date--header']/text()",
            "//span[@class='article__date']/text()"
        ]
        
        for selector in pubtime_selectors:
            pubtime = response.xpath(selector).extract_first()
            if pubtime:
                items.pubtime = pubtime.strip()
                break
        
        # 提取作者 - 更新为我们分析的选择器
        author_selectors = [
            "//span[@class='meta__author meta__author--no-after']/text()",
            "//span[@class='article__author']/text()",
            "//div[@class='article__author']/text()",
            "//span[@class='author']/text()",
            "//span[@class='meta__author meta__author--page']/text()"
        ]
        
        for selector in author_selectors:
            author = response.xpath(selector).extract_first()
            if author:
                items.author = author.strip()
                break
        
        if not items.author:
            items.author = ''
        
        # 确保所有必要字段都有值
        if not items.title:
            items.title = "Untitled"
        
        if not items.content:
            # 尝试提取其他可能的内容
            alternative_content = response.xpath("//div[@class='article__content']//text()").extract()
            items.content = "".join(alternative_content).strip().replace("\r", '').replace("\n", '') or "No content"
        
        # 打印调试信息
        print(f"标题: {items.title}")
        print(f"链接: {items.article_url}")
        print(f"发布时间: {items.pubtime}")
        print(f"内容长度: {len(items.content)}")
        
        if items.content and items.content != "No content":
            yield items


if __name__ == "__main__":
    FranceCoralSpider().start()
