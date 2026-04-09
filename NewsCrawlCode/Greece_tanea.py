# -*- coding: utf-8 -*-
"""

本地运行

"""
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
    sql = """ select db_name from keywords where country='Greece' and language = '希腊语'"""
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

    country = 'Greece'
    table = 'Greece'
    #希腊语
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
    keywords = db.find(f"select keywords_list from keywords where language = '希腊语' and country='{table}'", to_json=True)[0].get(
        "keywords_list")
    if isinstance(keywords, str):
        keywords = json.loads(keywords)
    print("待抓取的关键词列===========>", keywords)
    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = f"https://www.tanea.gr/search/{keyword}"

            yield feapder.Request(url,callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "referer": "https://www.tanea.gr/search/%CE%9A%CE%B1%CF%8D%CF%83%CF%89%CE%BD%CE%B1%CF%82/page/2/",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"135\", \"Not-A.Brand\";v=\"8\", \"Chromium\";v=\"135\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
        }
        request.cookies = {
            "_ga": "GA1.1.863642709.1743991450",
            "compass_uid": "11752599-81e4-47f1-ab77-0740595a52ec",
            "_cb": "BisaQFxwlYzDtlwVX",
            "cto_bundle": "tr46Sl80JTJGSTBwSU94TldxNlhmemdzV1cyQWVIaiUyRng4cHl6b0tXSThDYmxVM2RoR25DSGxhJTJCZHJHWGZYZ0RxdDU5VFBEOWNleTJ6cWhIejVpeGpQanp1RVg2Q1p2WG5rbkttQXE2V1JaQ3NLUTlRMTRMbWZkWm94cU9PaXR5aDV1V09IRXBtTkNTVFFqdDEzNlpwRTJhVkF2a1ElM0QlM0Q",
            "connectId": "{\"ttl\":86400000,\"lastUsed\":1743991493101,\"lastSynced\":1743991493101}",
            "__gads": "ID=aefe6212d826aaec:T=1743991421:RT=1743991753:S=ALNI_Mb5nVuaR3hkJWB0bjSdM6yJR4_14Q",
            "__gpi": "UID=0000108e0e4e91e6:T=1743991421:RT=1743991753:S=ALNI_MaJtO0QLtwWGe9Z4lIwGk_elaNw5g",
            "__eoi": "ID=e837db969612a8fe:T=1743991421:RT=1743991753:S=AA-AfjafeO7TX86_pUOlofvrekqb",
            "_cb_svref": "external",
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "panoramaId": "c254b407f9df76bd201fd1b209d116d539385cadd59156c5fc78cb8587015291",
            "panoramaIdType": "panoIndiv",
            "weather_api_cookie": "athensgr",
            "___nrbi": "%7B%22firstVisit%22%3A1743991450%2C%22userId%22%3A%2211752599-81e4-47f1-ab77-0740595a52ec%22%2C%22userVars%22%3A%5B%5D%2C%22futurePreviousVisit%22%3A1744078417%2C%22timesVisited%22%3A2%7D",
            "_chartbeat2": ".1743991450808.1744078745570.11.CYDuhzDqUKxnDzBNusD_tQGbD4tfr3.6",
            "_chartbeat5": "",
            "panoramaId_expiry": "1744683496111",
            "FCNEC": "%5B%5B%22AKsRol_24CYssGHAJladlO7cJGClLa5RD3-_wYgHIPsmyd4MnEDVYI6EKvm-YBiqqlZApBdBoWfSCyhpQNrjqjtcQl7HOZzgHEns-pZfZMoLbseYLLvb6z-pl54N-GZyHcoELcR-N_lagLXThgSwaKlCG-7Nqu74jg%3D%3D%22%5D%5D",
            "___nrbic": "%7B%22previousVisit%22%3A1743991450%2C%22lastBeat%22%3A1744078753%2C%22currentVisitStarted%22%3A1744078417%2C%22sessionId%22%3A%2203dbac17-f8b4-4608-adde-2ef72647650f%22%2C%22sessionVars%22%3A%5B%5D%2C%22visitedInThisSession%22%3Atrue%2C%22pagesViewed%22%3A6%2C%22landingPage%22%3A%22https%3A//www.tanea.gr/%22%2C%22referrer%22%3A%22%22%2C%22lpti%22%3Anull%7D",
            "_chartbeat4": "t=aKff8Crkl1vBqDFZcfx0b8DUaJ3I&E=8&x=0&c=0.14&y=17225&w=762",
            "_ga_1NX1SGSEPE": "GS1.1.1744078416.2.1.1744078753.35.0.0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//a[@data-link-name='article']/@href").extract()
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
            if "video" in item:
                continue
            items = Item()
            print(item)
            items.table_name = self.table # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url =item
            # items.title = item.get("post_title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://www.tanea.gr/search/{current_keyword}"

        yield feapder.Request(url,callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='wrap-postbody']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
