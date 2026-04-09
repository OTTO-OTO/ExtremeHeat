# -*- coding: utf-8 -*-
"""

集群运行

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
    sql = """ select db_name from keywords where country='Samoa' and language = '英语'"""
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

    country = 'Samoa'
    table = 'Samoa'
    # 英语
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
    keywords = db.find(f"select keywords_list from keywords where language = '英语' and country='{table}'",to_json=True)[0].get(
        "keywords_list")
    print("待抓取的关键词列===========>", keywords)

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.samoaobserver.ws/stories/page/1.json"
            params = {
                "name": f"{keyword}",
                "api": "true"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=1, i",
            "referer": "https://www.samoaobserver.ws/search?name=heavy+rain",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0"
        }
        request.cookies = {
            "_samoa_observer": "e018953b5c3f5eb1c58aeae9d8b4f7ce",
            "_gid": "GA1.2.673318012.1738892701",
            "usprivacy": "1YNY",
            "_li_dcdm_c": ".samoaobserver.ws",
            "_lc2_fpi": "49d019c372ab--01jkf19xftyd8q8kja4ms0d7a6",
            "_lc2_fpi_meta": "%7B%22w%22%3A1738892703226%7D",
            "_lr_retry_request": "true",
            "_lr_env_src_ats": "false",
            "connectId": "%7B%22puid%22%3A%2244f6ffcc02cc0225ea2b14cf6bc56b817db8f82b7adbfc65820136c081cca024%22%2C%22vmuid%22%3A%22OlhX_L2-uYqwDp-FM6Lwg_GVHp0JPEfFpYy7wf_j4j5p2zqp6QSxKM7oGYWimjTemQ6fLOLmfJzeubfUyDYsdg%22%2C%22connectid%22%3A%22OlhX_L2-uYqwDp-FM6Lwg_GVHp0JPEfFpYy7wf_j4j5p2zqp6QSxKM7oGYWimjTemQ6fLOLmfJzeubfUyDYsdg%22%2C%22connectId%22%3A%22OlhX_L2-uYqwDp-FM6Lwg_GVHp0JPEfFpYy7wf_j4j5p2zqp6QSxKM7oGYWimjTemQ6fLOLmfJzeubfUyDYsdg%22%2C%22ttl%22%3A86400000%2C%22lastSynced%22%3A1738892703673%2C%22lastUsed%22%3A1738892703673%7D",
            "_scor_uid": "959670918e0e4e3fa22cd09b83e332bb",
            "panoramaId_expiry": "1739497484498",
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "panoramaId": "dd6967eb7842afd67768334a0c8d16d53938761497b942a43ecff32adb2f1512",
            "_lr_geo_location": "SG",
            "_ga": "GA1.2.1556317006.1738892701",
            "_ga_GT7GMJ8LCY": "GS1.1.1738892700.1.1.1738892739.21.0.0",
            "ccuid": "0a0aef2b-139b-40b2-83c2-bbe4f8a2942a"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        # print(type(response.text))
        # print(type(response.json))
        # links = json.loads(response.text)
        links = response.json.get("stories")
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
            items.table_name = self.table  # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url = "https://www.samoaobserver.ws/category/" + item.get("category_name") + "/" + str(
                item.get("id"))
            items.title = item.get("sub_heading")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            # items.pubtime = ''
            # items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://www.samoaobserver.ws/stories/page/{current_page}.json"
        params = {
            "name": f"{current_keyword}",
            "api": "true"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        # items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(
            response.xpath(
                "//div[@class='article__content text-new-brand-black py-0 leading-big']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("(//span[@class='text-sm']/text())[3]").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
