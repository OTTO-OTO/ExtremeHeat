# 付费
import json
import re
import time
import uuid
import random
import string

import feapder
from feapder import Item
from feapder.db.mysqldb import MysqlDB


class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.42.183", port=3306, db="spider_data", user_name="czm", user_pass="root"
    )
    # 创建表结构
    table = 'UK'
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

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[6, 10],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
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

    previous_links = None
    country = 'United Kingdom of Great Britain and Northern Ireland'
    keywords = ["Extreme", "Heat", "High", "Temperature", "Heavy", "Rain", "Drought", "Power", "Outage", "from", "Fire", "Air", "Pollution", "Climate", "Change", "Crop", "Yield", "Reduction", "Oxygen", "Deficiency", "Affecting", "Traffic", "Ecological", "Disaster", "Economy", "Marine", "Heatwave", "Coral", "stroke"]
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.google.co.uk/search"
            params = {
                "q": f"{keyword}",
                "as_sitesearch": "www.theguardian.com"
            }
            yield feapder.Request(url, params=params,callback=self.parse_url, page=page, keyword=keyword,filter_repeat=True)

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
    
    def generate_session_id(self):
        """生成随机会话ID"""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=32))
    
    def download_midware(self, request):
        # 随机选择用户代理
        user_agent = random.choice(self.user_agents)
        
        # 生成随机版本号
        edge_version = str(random.randint(120, 131))
        chrome_version = str(random.randint(120, 131))
        
        # 动态生成referer
        referer_keyword = random.choice(self.keywords)
        referer = f"https://www.google.co.uk/search?q={referer_keyword}&as_sitesearch=www.theguardian.com"
        
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
            "priority": "u=0, i",
            "referer": referer,
            "sec-ch-ua": f"\"Microsoft Edge\";v=\"{edge_version}\", \"Chromium\";v=\"{chrome_version}\", \"Not_A Brand\";v=\"24\"",
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
            "AEC": f"{self.generate_session_id()}",
            "__Secure-BUCKET": random.choice(["CJcH", "CJcI", "CJcJ"]),
            "NID": f"529={self.generate_session_id()}",
            "__Secure-STRP": f"{self.generate_session_id()}",
            "DV": f"{self.generate_session_id()}",
            "1P_JAR": f"{int(time.time())}",
            "SEARCH_SAMESITE": "CgQIoLcB",
            "SAMESITE_HIGH_SCHOOL": "CgQIoLcB",
            "HSID": f"{self.generate_session_id()}",
            "SSID": f"{self.generate_session_id()}",
            "APISID": f"{self.generate_session_id()}",
            "SAPISID": f"{self.generate_session_id()}"
        }
        
        # 随机延迟，模拟人类操作
        time.sleep(random.uniform(1, 3))
        
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        
        # 打印响应状态码和URL
        print(f"响应状态码: {response.status_code}")
        print(f"请求URL: {response.url}")
        print(f"响应内容长度: {len(response.text)}")
        
        # 保存响应内容用于调试
        if current_keyword == "Temperature" and request.page == 1:
            with open(f"google_search_{current_keyword}.html", "w", encoding="utf-8") as f:
                f.write(response.text)
            print(f"已保存搜索结果到google_search_{current_keyword}.html")
        
        # 尝试多种XPath选择器来提取链接
        links = []
        
        # 打印前2000个字符的响应内容，用于查看HTML结构
        if current_keyword == "Temperature" and request.page == 1:
            print("\n=== 响应内容前2000字符 ===")
            print(response.text[:2000])
            print("=== 响应内容前2000字符结束 ===\n")
        
        # 第一种选择器 - 基于用户提供的HTML结构
        selector1 = response.xpath("//a[@jsname='UWckNb' and @class='zReHs']/@href")
        if selector1:
            print(f"选择器1找到 {len(selector1)} 个链接")
            for href in selector1.extract():
                print(f"选择器1链接: {href}")
                if 'theguardian.com' in href:
                    # 处理可能的空格和特殊字符
                    href = href.strip()
                    # 移除可能的反引号
                    if href.startswith('`'):
                        href = href[1:]
                    if href.endswith('`'):
                        href = href[:-1]
                    links.append(href)
                    print(f"选择器1添加链接: {href}")
        
        # 第二种选择器 - 更通用的jsname选择器
        selector2 = response.xpath("//a[@jsname]/@href")
        if selector2:
            print(f"选择器2找到 {len(selector2)} 个链接")
            for href in selector2.extract():
                print(f"选择器2链接: {href}")
                if 'theguardian.com' in href:
                    # 处理可能的空格和特殊字符
                    href = href.strip()
                    # 移除可能的反引号
                    if href.startswith('`'):
                        href = href[1:]
                    if href.endswith('`'):
                        href = href[:-1]
                    links.append(href)
                    print(f"选择器2添加链接: {href}")
        
        # 第三种选择器 - 从搜索结果标题中提取
        selector3 = response.xpath("//h3[@class='LC20lb MBeuO DKV0Md']/../@href")
        if selector3:
            print(f"选择器3找到 {len(selector3)} 个链接")
            for href in selector3.extract():
                print(f"选择器3链接: {href}")
                if 'theguardian.com' in href:
                    # 处理可能的空格和特殊字符
                    href = href.strip()
                    # 移除可能的反引号
                    if href.startswith('`'):
                        href = href[1:]
                    if href.endswith('`'):
                        href = href[:-1]
                    links.append(href)
                    print(f"选择器3添加链接: {href}")
                elif 'url?q=' in href:
                    # 处理Google的重定向链接
                    import urllib.parse
                    parsed_url = urllib.parse.urlparse(href)
                    query_params = urllib.parse.parse_qs(parsed_url.query)
                    if 'q' in query_params:
                        real_url = query_params['q'][0]
                        if 'theguardian.com' in real_url:
                            # 处理可能的空格和特殊字符
                            real_url = real_url.strip()
                            # 移除可能的反引号
                            if real_url.startswith('`'):
                                real_url = real_url[1:]
                            if real_url.endswith('`'):
                                real_url = real_url[:-1]
                            links.append(real_url)
                            print(f"选择器3提取的真实链接: {real_url}")
        
        # 第四种选择器 - 使用正则表达式提取
        import re
        # 匹配Guardian链接的正则表达式
        guardian_pattern = r'https://www\.theguardian\.com/[\w\-\./]+'
        matches = re.findall(guardian_pattern, response.text)
        if matches:
            print(f"正则表达式找到 {len(matches)} 个链接")
            for match in matches:
                print(f"正则表达式链接: {match}")
                # 处理可能的空格和特殊字符
                match = match.strip()
                # 移除可能的反引号
                if match.startswith('`'):
                    match = match[1:]
                if match.endswith('`'):
                    match = match[:-1]
                links.append(match)
                print(f"正则表达式添加链接: {match}")
        
        # 去重
        links = list(set(links))
        print(f"去重后找到 {len(links)} 个链接")
        print(f"最终链接列表: {links}")
        
        current_page = request.page
        current_links = links
        
        if self.previous_links is not None and current_links == self.previous_links:
            print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理

        self.previous_links = current_links  # 更新上一页的链接列表

        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            items = Item()
            items.table_name = self.table
            items.article_url =item
            # items.title = item.get("headline")
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.google.co.uk/search"
        params = {
            "q": f"{current_keyword}",
            "as_sitesearch": "www.theguardian.com",
            "start": f"{((current_page - 1) * 10)}"
        }
        yield feapder.Request(url, params=params,callback=self.parse_url, page=current_page, keyword=current_keyword, filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@id='maincontent']//p/text()").extract()).strip().replace("\r",'').replace("\n",'')
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
