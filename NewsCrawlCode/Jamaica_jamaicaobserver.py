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
    sql = """ select db_name from keywords where country='Jamaica' and language = '英语'"""
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

    country = 'Jamaica'
    table = 'Jamaica'
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
    keywords = db.find(f"select keywords_list from keywords where language = '英语' and country='{table}'", to_json=True)[0].get(
        "keywords_list")
    print("待抓取的关键词列===========>", keywords)
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.jamaicaobserver.com/page/1/"
            params = {
                "showResults": "1",
                "Action": "Search",
                "Archive": "False",
                "Order": "Desc",
                "y": "",
                "from_date": "",
                "to_date": "",
                "type_get_category": "all",
                "orderby_from_url": "relevance",
                "s": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.jamaicaobserver.com/?showResults=1&Action=Search&Archive=False&Order=Desc&y=&from_date=&to_date=&type_get_category=all&orderby_from_url=relevance&s=heavy+rain",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "_ga": "GA1.1.838061680.1735960351",
            "_pk_id.187.f384": "0e1ce6ba6e18626f.1735960353.",
            "_pubcid": "37dee447-611f-496f-b355-cf5bc16ded2c",
            "_pubcid_cst": "zix7LPQsHA%3D%3D",
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "__qca": "P0-1028282957-1735960355822",
            "pbjs-unifiedid": "%7B%22TDID%22%3A%22ea499b40-6dd6-47bc-804c-49e968bdabef%22%2C%22TDID_LOOKUP%22%3A%22FALSE%22%2C%22TDID_CREATED_AT%22%3A%222025-01-04T03%3A12%3A31%22%7D",
            "pbjs-unifiedid_cst": "zix7LPQsHA%3D%3D",
            "_awl": "2.1735960797.5-e448c950efb405dcf1322af02352e538-6763652d617369612d6561737431-0",
            "sharedid": "1e062a9b-4498-4082-94d8-f4ffe738b014",
            "sharedid_cst": "zix7LPQsHA%3D%3D",
            "_lr_env_src_ats": "false",
            "trc_cookie_storage": "taboola%2520global%253Auser-id%3D3c13b441-4f28-404d-a71c-201ac821dadd-tucte722e5f",
            "_lc2_fpi": "d33c6690aa71--01jgqn7rjzyn19rn2daeaqarak",
            "_lc2_fpi_meta": "%7B%22w%22%3A1735960814175%7D",
            "cto_bundle": "SqSPVV9PT1JQUkJIcFlMNFJhcloxd0pnNGdTeXE2Q3FHYjRHbFVwYmZOTkQ3RSUyQmtaZ09EV0lScjdNYWZTNk5IaFpsdWJPU3VtZ3lobE56eEJidTZSMlVUek1DbSUyRnN1d2pGWVFDVkgyZW8yODlneDRmek84YVRBNlJWMVU0b21UMnVPQUhxTEdDc3RaYTRSNmlmRGM1NGE0Q3ozSDBPVWRmU2ZpQ0VyM2dlM2FSJTJCUWMlM0Q",
            "panoramaId_expiry": "1737794591835",
            "panoramaId": "c2ba4404dd1662779c299379ee5816d53938f08b6896985ba839d86735aa250d",
            "panoramaIdType": "panoIndiv",
            "__AP_SESSION__": "6bdcb78e-2708-4689-956a-7fbd1b7a55a5",
            "_pk_ses.187.f384": "1",
            "cto_bidid": "YZpsQ180Z3BVV3FiemJUT0lraHIwekdjcndmR3pEaWJJMmh1MGYxSjhodjJCT1VNdXdJaFBWcVozdERST3kwRHpsRmp3eWduNlIyeFRMY0lPbE1EZ3FoV3NvZkRUYldqQ0laWkYlMkZYZlJHQ3MxRFhRa2ZvOSUyRjZsV3dsMVgwVzl1eDFqZEI",
            "cto_dna_bundle": "gRQ57F9PT1JQUkJIcFlMNFJhcloxd0pnNGdSZjV4Y3BGUGg5dk5mZU01djluS0glMkJKUzQ4QmYzM2xMTm9rVk9LbEVQaHNFOVpUOFU5U0dIanlrUHBDYlppbDhBJTNEJTNE",
            "_ga_SFTZ8R89PJ": "GS1.1.1737342667.3.1.1737342779.22.0.0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")

        links = response.xpath(
            "//div[@class='template-content']//article//div[@class='title']/a/@href").extract()
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
            items = Item()
            items.table_name = self.table
            items.article_url = item
            # items.title = item.get("headline")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://www.jamaicaobserver.com/page/{current_page}/"
        params = {
            "showResults": "1",
            "Action": "Search",
            "Archive": "False",
            "Order": "Desc",
            "y": "",
            "from_date": "",
            "to_date": "",
            "type_get_category": "all",
            "orderby_from_url": "relevance",
            "s": f"{current_keyword}"
        }
        yield feapder.Request(url, params, callback=self.parse_url, page=current_page, keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//article[@class='article article_0  ']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
