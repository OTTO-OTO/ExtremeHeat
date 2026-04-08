# 付费
import json
import re
import time
import uuid
import random
import string
import urllib.parse

import feapder
from feapder import Item
from feapder.db.mysqldb import MysqlDB
from playwright.sync_api import sync_playwright


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
            # 使用谷歌搜索卫报内容
            url = "https://www.google.co.uk/search"
            params = {
                "q": f"{keyword} site:www.theguardian.com",
                "start": str((page - 1) * 10)  # 谷歌搜索每页10条结果
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword, filter_repeat=True)

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
        current_page = request.page
        print(f"当前关键词{current_keyword}的页数为:{current_page}")
        
        # 构建完整的谷歌搜索URL
        url = "https://www.google.co.uk/search"
        params = {
            "q": f"{current_keyword} site:www.theguardian.com",
            "start": str((current_page - 1) * 10)  # 谷歌搜索每页10条结果
        }
        full_url = url + "?" + urllib.parse.urlencode(params)
        print(f"请求URL: {full_url}")
        
        # 使用Playwright抓取页面
        links = []
        try:
            with sync_playwright() as p:
                # 启动浏览器，使用更真实的配置
                browser = p.chromium.launch(
                    headless=True,  # 无头模式，适合服务器环境
                    args=[
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-blink-features=AutomationControlled",
                        "--enable-features=NetworkService,NetworkServiceInProcess",
                        "--disable-features=site-per-process",
                        "--window-size=1920,1080",
                        "--lang=en-GB"
                    ]
                )
                # 创建新的页面
                page = browser.new_page()
                
                # 增加更长的随机延迟，模拟人类操作
                time.sleep(random.uniform(3, 6))
                
                # 设置更真实的用户代理和请求头
                user_agent = random.choice(self.user_agents)
                page.set_extra_http_headers({
                    "User-Agent": user_agent,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1"
                })
                
                # 访问页面
                page.goto(full_url, wait_until="networkidle", timeout=60000)
                
                # 随机延迟，模拟人类浏览
                time.sleep(random.uniform(3, 6))
                
                # 模拟鼠标移动和滚动
                page.mouse.move(random.randint(100, 300), random.randint(100, 300))
                time.sleep(random.uniform(1, 2))
                page.mouse.move(random.randint(400, 600), random.randint(200, 400))
                time.sleep(random.uniform(1, 2))
                
                # 模拟页面滚动
                for i in range(3):
                    page.mouse.wheel(0, random.randint(100, 300))
                    time.sleep(random.uniform(0.5, 1.5))
                
                # 保存页面内容用于调试
                if current_keyword == "Extreme" and current_page == 1:
                    with open(f"google_search_{current_keyword}_playwright.html", "w", encoding="utf-8") as f:
                        f.write(page.content())
                    print(f"已保存谷歌搜索结果到google_search_{current_keyword}_playwright.html")
                
                # 从谷歌搜索结果中提取卫报的新闻链接
                # 使用用户提供的元素结构
                elements = page.query_selector_all("a.jsname='UWckNb'")
                print(f"使用选择器 a.jsname='UWckNb' 找到 {len(elements)} 个链接")
                
                # 尝试另一种选择器
                if not elements:
                    elements = page.query_selector_all("a.zReHs")
                    print(f"使用选择器 a.zReHs 找到 {len(elements)} 个链接")
                
                # 尝试更通用的选择器
                if not elements:
                    elements = page.query_selector_all("a[href*='theguardian.com']")
                    print(f"使用选择器 a[href*='theguardian.com'] 找到 {len(elements)} 个链接")
                
                for element in elements:
                    try:
                        href = element.get_attribute("href")
                        if not href:
                            continue
                        
                        # 清理链接，移除可能的空格和反引号
                        href = href.strip().replace('`', '')
                        
                        # 确保是卫报的链接
                        if 'theguardian.com' in href:
                            # 处理可能的URL参数
                            if 'url=' in href:
                                # 从google的ping参数中提取真实链接
                                import re
                                match = re.search(r'url=([^&]+)', href)
                                if match:
                                    href = urllib.parse.unquote(match.group(1))
                            
                            # 确保URL格式正确
                            if not href.startswith('http'):
                                if href.startswith('/'):
                                    href = 'https://www.theguardian.com' + href
                                else:
                                    continue
                            
                            # 过滤掉非新闻链接
                            if any(pattern in href for pattern in ['/news/', '/sport/', '/commentisfree/', '/culture/', '/business/', '/environment/']):
                                links.append(href)
                                print(f"提取到卫报链接: {href}")
                    except Exception as e:
                        print(f"处理链接时出错: {e}")
                
                # 关闭浏览器
                browser.close()
        except Exception as e:
            print(f"Playwright抓取页面时出错: {e}")
        
        # 去重
        links = list(set(links))
        print(f"去重后找到 {len(links)} 个链接")
        print(f"最终链接列表: {links}")
        
        # 检查链接是否与上一页相同
        if self.previous_links is not None and links == self.previous_links:
            print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            self.previous_links = None  # 重置上一页链接，准备处理下一个关键词
            return None

        self.previous_links = links  # 更新上一页的链接列表

        if not links:
            print(f"关键词 {current_keyword} 的第 {current_page} 页没有数据，退出当前关键字的循环")
            self.previous_links = None  # 重置上一页链接，准备处理下一个关键词
            return None
        
        # 处理每个链接
        for item in links:
            items = Item()
            items.table_name = self.table
            items.article_url = item
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        # 生成下一页请求
        next_page = current_page + 1
        yield feapder.Request(
            url=url, 
            params={
                "q": f"{current_keyword} site:www.theguardian.com",
                "start": str((next_page - 1) * 10)
            }, 
            callback=self.parse_url, 
            page=next_page, 
            keyword=current_keyword, 
            filter_repeat=True
        )

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
