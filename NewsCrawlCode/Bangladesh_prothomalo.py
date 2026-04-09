# -*- coding: utf-8 -*-
# 173
import re

import feapder
from feapder import Item
import json
from lxml import etree

from feapder.db.mysqldb import MysqlDB

class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.101.200", port=3307, db="spider_keywords", user_name="czm", user_pass="root"
    )
    sql = """ select db_name from keywords where country='Bangladesh' and language='英语'"""
    mysql_db = db.find(sql, to_json=True)[0].get("db_name")
    print("待写入的数据库是:", mysql_db)
    # 判断数据库是否存在
    db.execute(f"CREATE DATABASE IF NOT EXISTS `{mysql_db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[8, 10],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB=f"{mysql_db}",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Bangladesh'
    table = 'Bangladesh'
    #英语
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
    keywords = db.find(f"select keywords_list from keywords where language = '英语' and country='{table}'", to_json=True)[0].get(
        "keywords_list")
    print("待抓取的关键词列===========>", keywords)
    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://en.prothomalo.com/api/v1/advanced-search"
            params = {
                # "section-id": "16623,16767,16768",
                "q": f"{keyword}",
                "type": "text,team-bio,listicle,",
                "offset": "15",
                "limit": "10",
                # "fields": "headline,subheadline,slug,url,tags,hero-image-s3-key,hero-image-caption,hero-image-metadata,last-published-at,alternative,authors,author-name,author-id,sections,story-template,metadata,hero-image-attribution,access"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://www.gdnlife.com",
            "Referer": "https://www.gdnlife.com/?p=food",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        request.cookies = {
            "ASP.NET_SessionId": "pq1zalujc5rpa3fdfyrqz4ll",
            "BNES_ASP.NET_SessionId": "SfRsJqxEvkgTXz2ukkJk1Ev3Uf4p4fhJJmGxJyvrIsOV2nY3zY+Pvxxsxv9y2wL2X3IbWpz+ZkfUiUWHMa2AjoKvECWz7rvU",
            "_gid": "GA1.2.1519376077.1737176826",
            "_gat_gtag_UA_156248549_1": "1",
            "fpestid": "ayUOn2pZHzYsdMY_Ojvnn14zaguzhRu63dE72fLtalbne1Jj-LtyVPJbpu8YPc6FuDBaHA",
            "_gat_gtag_UA_236842100_1": "1",
            "_ga_KD61VSXXYZ": "GS1.1.1737176817.1.1.1737177439.60.0.0",
            "_ga_14ED973B8N": "GS1.1.1737176832.1.1.1737177439.0.0.0",
            "_ga": "GA1.2.551413796.1737176818",
            "_ga_M3DNCTT38J": "GS1.1.1737176836.1.1.1737177465.13.0.0",
            "BNES__gid": "2B70qp7OPCqT3Y3HwDbcg9+bCP3el+iJqGG0P27xLbwkugM1HWazySEl4b6lOrSHBHOGq33CMtFiyxHLFdsh0g==",
            "BNES_fpestid": "o2PHJHDavqmZ2sn1MFxJfVyagCuKes5O6yOm0PTvgXjG4U3NhGktZZaQH+ajota3eO9lkrlwg3WoeiIWd7aEJZxesLAjERdI+VZ6PtelJmG/W2fo0L3ideY4QkGU5/MpopTV5iHlmlKx/jZiQ8dAbQ==",
            "BNES__gat_gtag_UA_156248549_1": "3mlLZQLvHGs5B7Im9fi2CrQSmvyQ5i1lX/rH2raa8R+ZFxyBlj1f3Cl9DUOYdpAi/iZhMU5a0CI=",
            "BNES__gat_gtag_UA_236842100_1": "y7WttTGE38ooE9x3BSnhSrhMxcZ/3PW7kxZ2shWf2mpSA65nu/wPdFM9YTfYxLMddC2nItqdKcI=",
            "BNES__ga_KD61VSXXYZ": "QbSN7JzbJLNw4u6dhQFN3jTMR6CWJnfA1B1qLkFiegGWE3jfj4fUdXfuYSWfadB49+rD2qEOamHO8+kOFEN1k9KEMWWSg+fkgjG8ZSPCKA3t0PGnWBZjFw==",
            "BNES__ga_14ED973B8N": "wBijrCz/nZ7iQEyOr7XWYjbWa7Zvme4XZzu4EfV0ziY2hKo9HKNKybFA0J53gJPuycED6gmhXZRVfAWl0UF32qhQbFp9PLjyh2h7PaTTZfQ=",
            "BNES__ga_M3DNCTT38J": "IsJfVfKx8oktd8xyeL275M2ro7h4+aabEFgin7yej7leticQPKDwI7Qd3otvgYz/FkfoImG+aiF2O9zWgUOPt+i5mjpv8HDzj4S2SHR0E0K/tCpQzVis7w==",
            "BNES__ga": "IN9KpUvGS48Y8Z9axjbQK9Li1TlBj3gafwL0BI00LuVDFNelYhamtzVPzl6NJyfurbRJGOLs/LuohDA47/p8+w=="
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")

        links = response.json['items']
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
            items.article_url = item.get("url")
            items.title = item.get("headline")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 10
        url = "https://en.prothomalo.com/api/v1/advanced-search"
        params = {
            # "section-id": "16623,16767,16768",
            "q": f"{current_keyword}",
            "type": "text,team-bio,listicle,",
            "offset": f"{current_page}",
            "limit": "10",
            # "fields": "headline,subheadline,slug,url,tags,hero-image-s3-key,hero-image-caption,hero-image-metadata,last-published-at,alternative,authors,author-name,author-id,sections,story-template,metadata,hero-image-attribution,access"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        # items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='story-element story-element-text']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@dateTime").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
