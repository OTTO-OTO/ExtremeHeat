# 付费
import json
import re
import uuid

import feapder
from feapder import Item
from feapder.db.mysqldb import MysqlDB


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

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[6, 10],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
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
    )

    previous_links = None
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

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.francetvinfo.fr/recherche/1.html"
            params = {
                "request": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.francetvinfo.fr/recherche/2.html?request=Temp%C3%A9rature",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "service-worker-navigation-preload": "true",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
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
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//li[@class='home-search__item']/article/a/@href").extract()
        print(links)
        # # 输出匹配的值
        # for match in matches:
        #     print(match)

        # print(links)
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
            items.article_url = item
            # items.title = item.get("headline")
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://www.francetvinfo.fr/recherche/{current_page}.html"
        params = {
            "request": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1[@class='headArticle__title']/text()").extract_first()
        items.content = "".join(
            response.xpath("//div[@class='article__body']//p//text()").extract()) if response.xpath(
            "//div[@class='article__body']//p//text()").extract() else "".join(response.xpath(
            "//div[contains(@class, 'p-article__body') or contains(@class, 'c-body') or contains(@class, 'c-chapo')]//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
