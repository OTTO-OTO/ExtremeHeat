# -*- coding: utf-8 -*-
"""

本地运行

"""
import re
from datetime import datetime

import feapder
from feapder import Item
import json
import time
from lxml import etree
from sympy import print_tree


from feapder.db.mysqldb import MysqlDB

class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.101.200", port=3307, db="spider_keywords", user_name="czm", user_pass="root"
    )
    sql = """ select db_name from keywords where country='Uganda' and language='英语'"""
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

    country = 'Uganda'
    table = 'Uganda'
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
    # keywords = ['storm']
    print("待抓取的关键词列===========>", keywords)
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://public-api.wordpress.com/rest/v1.3/sites/240072615/search"
            params = {
                "fields[0]": "date",
                "fields[1]": "permalink.url.raw",
                "fields[2]": "tag.name.default",
                "fields[3]": "category.name.default",
                "fields[4]": "post_type",
                "fields[5]": "shortcode_types",
                "fields[6]": "forum.topic_resolved",
                "fields[7]": "has.image",
                "fields[8]": "image.url.raw",
                "fields[9]": "image.alt_text",
                "highlight_fields[0]": "title",
                "highlight_fields[1]": "content",
                "highlight_fields[2]": "comments",
                "query": f"{keyword}",
                "sort": "score_default",
                "page_handle": "",
                "size": "10"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, method='GET',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "origin": "https://observer.ug",
            "priority": "u=1, i",
            "referer": "https://observer.ug/",
            "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Microsoft Edge\";v=\"133\", \"Chromium\";v=\"133\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json['results']
        next_page_handle = response.json['page_handle']
        print(next_page_handle)
        current_page = request.page

        current_links = next_page_handle
        if self.previous_links is not None and current_links == self.previous_links:
            print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理
        self.previous_links = current_links  # 更新上一页的链接列表

        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            items = Item()
            items.table_name = self.table  # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.title = item.get("highlight").get("title")[0]
            items.pubtime = item.get("fields").get("date")
            items.article_url = "https://" + item.get("fields").get("permalink.url.raw")
            print(items.article_url,"<=========================")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://public-api.wordpress.com/rest/v1.3/sites/240072615/search"
        params = {
            "fields[0]": "date",
            "fields[1]": "permalink.url.raw",
            "fields[2]": "tag.name.default",
            "fields[3]": "category.name.default",
            "fields[4]": "post_type",
            "fields[5]": "shortcode_types",
            "fields[6]": "forum.topic_resolved",
            "fields[7]": "has.image",
            "fields[8]": "image.url.raw",
            "fields[9]": "image.alt_text",
            "highlight_fields[0]": "title",
            "highlight_fields[1]": "content",
            "highlight_fields[2]": "comments",
            "query": f"{current_keyword}",
            "sort": "score_default",
            "page_handle": f"{next_page_handle}",
            "size": "10"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, method='GET',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        # items.title = response.xpath("//meta[@property='og:title']/@content").extract_first()
        items.content = "".join(
            response.xpath("//div[@class='pf-content']//p/text() | //div[@class='entry-content']//p/text()").extract())
        items.author = ''
        # items.pubtime = response.xpath("//input[@class='page-date-published']/@value").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
