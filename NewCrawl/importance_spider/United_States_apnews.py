# -*- coding: utf-8 -*-
"""

本地运行

"""
import re
import time

import feapder
from feapder import Item
import json
from lxml import etree

from feapder.db.mysqldb import MysqlDB

class AirSpiderDemo(feapder.AirSpider):
    def __init__(self):
        super().__init__()
        # 初始化数据库连接
        self.db = MysqlDB(
            ip="192.168.42.183", port=3306, db="spider_data", user_name="czm", user_pass="root"
        )
        # 固定使用 spider_data 数据库
        self.mysql_db = "spider_data"
        print("待写入的数据库是:", self.mysql_db)
        # 判断数据库是否存在
        self.db.execute(f"CREATE DATABASE IF NOT EXISTS `{self.mysql_db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")
        # 创建表结构
        self.country = 'United States'
        self.table = 'United_States'
        create_table_sql = f"""
                        CREATE TABLE IF NOT EXISTS `{self.mysql_db}`.`{self.table}`  (
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
        self.db.execute(create_table_sql)
        print(f"{self.table}创建成功<=================")
        # 打印关键词列表
        print("待抓取的关键词列===========>", self.keywords)

    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=3,  # 减少并发线程数，降低被检测的风险
        # 增加请求间隔时间，避免被速率限制
        SPIDER_SLEEP_TIME=[20, 25],
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
        # 禁用SSL验证，避免连接问题
        VERIFY_SSL=False,
    )
    previous_links = None
    # 使用硬编码的关键词列表
    keywords = ["Extreme", "Heat", "High", "Temperature", "Heavy", "Rain", "Drought", "Power", "Outage", "from", "Fire", "Air", "Pollution", "Climate", "Change", "Crop", "Yield", "Reduction", "Oxygen", "Deficiency", "Affecting", "Traffic", "Ecological", "Disaster", "Economy", "Marine", "Heatwave", "Coral", "Stroke"]
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://apnews.com/search"
            params = {
                "q": f"{keyword}",
                "p": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        import random
        # 随机User-Agent列表
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
        ]
        
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "sec-ch-ua": "\"Google Chrome\";v=\"130\", \"Not=A?Brand\";v=\"24\", \"Chromium\";v=\"130\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": random.choice(user_agents)
        }
        
        # 简单的cookies，避免使用过期的cookies
        request.cookies = {
            "_ga": "GA1.1.1234567890.1234567890",
            "_gid": "GA1.2.1234567890.1234567890",
            "OptanonAlertBoxClosed": "2025-01-01T00:00:00.000Z",
            "OptanonConsent": "isGpcEnabled=0&datestamp=Wed+Jan+01+2025+00%3A00%3A00+GMT%2B0000&version=202411.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=12345678-1234-1234-1234-123456789012&interactionCount=1&isAnonUser=0&landingPath=NotLandingPage&groups=1%3A1%2C2%3A1%2C3%3A1%2C4%3A1&intType=1"
        }
        
        # 禁用SSL验证
        request.verify = False
        
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        
        # 尝试多种方式提取链接
        links = []
        
        # 方式1：直接提取包含article的链接
        links.extend(response.xpath("//a[contains(@href, 'apnews.com/article')]/@href").extract())
        
        # 方式2：提取所有a标签，然后筛选包含article的链接
        all_links = response.xpath("//a/@href").extract()
        for link in all_links:
            if "apnews.com/article" in link and link not in links:
                links.append(link)
        
        # 方式3：提取可能的相对链接并转换为绝对链接
        relative_links = response.xpath("//a[contains(@href, '/article')]/@href").extract()
        for link in relative_links:
            if link.startswith('/'):
                full_link = "https://apnews.com" + link
                if full_link not in links:
                    links.append(full_link)
        
        # 去重，只保留唯一链接
        links = list(set(links))
        print(links)
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
            if "video" in item:
                continue
            items = Item()
            print(item)
            items.table_name = self.table
            items.article_url = item
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items,page=request.page)

        current_page = request.page + 1
        url = "https://apnews.com/search"
        params = {
            "q": f"{current_keyword}",
            "p": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        
        # 解析页面内容
        tree = etree.HTML(response.text)
        
        # 尝试从用户提供的class中提取标题
        title = tree.xpath("//h1[@class='Page-headline']/text()")
        title = title[0].strip() if title else None
        
        # 如果没有找到，尝试使用通用选择器
        if not title:
            title = tree.xpath("//h1/text()")
            title = title[0].strip() if title else None
        
        items.title = title
        
        # 尝试从用户提供的class中提取内容
        content_elements = tree.xpath("//div[@class='Page-storyBody gtmMainScrollContent']//p/text()")
        
        # 如果没有找到，尝试使用原来的选择器
        if not content_elements:
            content_elements = tree.xpath("//div[@class='RichTextStoryBody RichTextBody']//p/text()")
        
        # 如果仍然没有找到，尝试通用选择器
        if not content_elements:
            content_elements = tree.xpath("//article//p/text()")
        
        items.content = "".join(content_elements).strip()
        items.author = ''
        
        # 提取发布时间
        pubtime = tree.xpath("//meta[@property='article:published_time']/@content")
        items.pubtime = pubtime[0] if pubtime else None
        
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()