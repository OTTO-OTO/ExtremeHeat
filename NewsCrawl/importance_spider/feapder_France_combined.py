import json
import re
import time
import uuid

import feapder
from feapder import Item
from feapder.db.mysqldb import MysqlDB
from lxml import etree


class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.42.97", port=3306, db="spider_data", user_name="czm", user_pass="root"
    )
    # 创建表结构
    table = 'France'
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

        SPIDER_THREAD_COUNT=3,  # 减少并发线程数，降低被检测的风险
        # 增加请求间隔时间，避免被速率限制
        SPIDER_SLEEP_TIME=[10, 15],
        SPIDER_MAX_RETRY_TIMES=3,  # 增加重试次数
        MYSQL_IP="192.168.42.97",
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
        # 禁用SSL验证，避免连接问题
        VERIFY_SSL=False,
    )

    previous_links = {}
    previous_tokens = {}
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
        "Coral", "Corail"
    ]
    print("待抓取的关键词列===========>", keywords)
    
    # 定义不同网站的配置
    sites = [
        {
            "name": "francetvinfo",
            "search_url": "https://www.francetvinfo.fr/recherche/1.html",
            "method": "GET",
            "content_type": "html",
            "link_selector": "//li[@class='home-search__item']/article/a/@href",
            "content_selector": "//div[@class='article__body']//p//text()",
            "content_selector_fallback": "//div[contains(@class, 'p-article__body') or contains(@class, 'c-body') or contains(@class, 'c-chapo')]//p/text()",
            "pubtime_selector": "//time/@datetime"
        }
    ]
    
    def start_requests(self):
        for site in self.sites:
            for keyword in self.keywords:
                page = 1
                if site["name"] == "francetvinfo":
                    url = site["search_url"]
                    params = {
                        "request": f"{keyword}"
                    }
                    yield feapder.Request(
                        url, 
                        params=params, 
                        callback=self.parse_url, 
                        page=page, 
                        keyword=keyword, 
                        site=site, 
                        filter_repeat=True
                    )

    def download_midware(self, request):
        import random
        # 随机User-Agent列表
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
        ]
        
        site = request.site
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": random.choice(user_agents)
        }
        
        if site["name"] == "francetvinfo":
            request.headers["referer"] = "https://www.francetvinfo.fr/recherche/2.html?request=Temp%C3%A9rature"
            request.headers["service-worker-navigation-preload"] = "true"
            request.cookies = {
                "nli": "2f8152d8-65b0-3b12-5572-b16e2b82f7b6",
                "ftvi-web-push-popin-display": "true",
                "_gcl_au": "1.1.1521430147.1734663581",
                "_cb": "9y168BE-ibpCV9o8i",
                "_fbp": "fb.1.1734663581777.254427408141341740",
                "pa_privacy": "%22optin%22",
                "_chartbeat2": ".1734663580832.1734664316654.1.3aTtOfoX51CV9rh7BM54daCrtPK.6",
                "_pcid": "%7B%22browserId%22%3A%22m4w6hy2w69bvph5e%22%2C%22_t%22%3A%22mkklfff0%7Cm4w6hy30%22%7D",
                "_pctx": "%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAE0RXSwH18yBbANaDUAM3EAGAD78ALAHcAbAAsAngGYJIAL5A",
                "ak_bmsc": "E934EE8D6822BCFEAAD73B6D24451F1D~000000000000000000000000000000~YAAQeGt7XCRiOESUAQAACn3iaBoOjv56mFK4WtqDqG/bA6WAfNIGrIdlB0E+p6Ylf8Pu66Po71T688UCORNKQdVJLYkRkBvIRHTPO57PEbJUEpW+kWbt+UYKiRigtxE51hYGfWUehNqVGQmP02IBrWbUqdGFP0p8xhpgKjvGGzEMkWjKQsykQZAIXuXP8GGF0h96ErShiRw1oFSZW/Ben5XcZUmiX5zso7rXoGDPrQ/+ehzxN+ZWmJtTgYY1yzFBr9SD3TYe0cO8a+R76nS2JsnJBu7+tTWwgT+7XtZd7By9g85OY8xUcySfzx3iowUo3ZQgOhFkT875BLQ9brv6cgQPeQlvn5WmTQPpgDHh0Q5UnLQOntTwyZXuxCdGdECU5GWkxvCUHj3YgxydQ1vU82Kuv884h7c=",
                "_cs_mk_pa": "0.872778323274171_1736926633951"
            }
        
        # 禁用SSL验证
        request.verify = False
        
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        site = request.site
        print(f"当前网站{site['name']}，关键词{current_keyword}的页数为:{request.page}")
        
        links = []
        
        if site["name"] == "francetvinfo":
            links = response.xpath(site["link_selector"]).extract()
        
        current_page = request.page
        
        # 检查是否有新内容
        key = f"{site['name']}_{current_keyword}"
        if key in self.previous_links and self.previous_links[key] == links:
            print(f"网站 {site['name']} 关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None
        self.previous_links[key] = links
        
        if not links:
            print(f"网站 {site['name']} 关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None
            
        for item in links:
            article_url = item
            title = None
            
            # 跳过视频链接
            if article_url and "video" in article_url:
                continue
            
            items = Item()
            items.table_name = self.table
            items.article_url = article_url
            items.title = title
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=article_url, callback=self.parse_detail, items=items, site=site)

        # 翻页
        current_page = request.page + 1
        if site["name"] == "francetvinfo":
            url = f"https://www.francetvinfo.fr/recherche/{current_page}.html"
            params = {
                "request": f"{current_keyword}"
            }
            yield feapder.Request(
                url, 
                params=params, 
                callback=self.parse_url, 
                page=current_page, 
                keyword=current_keyword, 
                site=site, 
                filter_repeat=True
            )

    def parse_detail(self, request, response):
        items = request.items
        site = request.site
        items.table_name = self.table
        
        # 如果标题为空，从页面提取
        if not items.title:
            items.title = response.xpath("//h1[@class='headArticle__title']/text()").extract_first()
        
        # 提取文章内容
        content_elements = response.xpath(site["content_selector"]).extract()
        if not content_elements:
            # 尝试fallback选择器
            if "content_selector_fallback" in site:
                content_elements = response.xpath(site["content_selector_fallback"]).extract()
            # 尝试通用选择器
            if not content_elements:
                content_elements = response.xpath("//article//p/text()").extract()
        items.content = "".join(content_elements).strip().replace("\r", '').replace("\n", '')
        
        items.author = ''
        items.pubtime = response.xpath(site["pubtime_selector"]).extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()