# -*- coding: utf-8 -*-
"""

集群运行

"""
import re

import feapder
from feapder import Item
import json
import time
from lxml import etree
from feapder.db.mysqldb import MysqlDB


class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.101.200", port=3307, db="spider_keywords", user_name="czm", user_pass="root"
    )
    sql = """ select db_name from keywords where country='Nicaragua' and language = '西班牙语'"""
    mysql_db = db.find(sql, to_json=True)[0].get("db_name")
    print("待写入的数据库是:", mysql_db)
    # 判断数据库是否存在
    db.execute(f"CREATE DATABASE IF NOT EXISTS `{mysql_db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=8,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[1, 2],
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

    country = 'Nicaragua'
    table = 'Nicaragua'
    # 西班牙语
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
    keywords = db.find(f"select keywords_list from keywords where language = '西班牙语' and country='{table}'", to_json=True)[0].get(
        "keywords_list")
    if isinstance(keywords, str):
        keywords = json.loads(keywords)
    print("待抓取的关键词列===========>", keywords)
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://confidencial.digital/page/1/"
            params = {
                "s": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, method='GET',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "cache-control": "max-age=0",
            "content-type": "application/x-www-form-urlencoded",
            "origin": "https://confidencial.digital",
            "priority": "u=0, i",
            "referer": "https://confidencial.digital/page/6/?s=calor&__cf_chl_tk=UDXDfsxt.eWhmC8fZm8Nu6DbFSbx9ORSFqd.8vmkSXs-1744607351-1.0.1.1-EUpb.U.xlbTfebyfoFyWgqPU6erEXl1r2Mq_H6WybW8",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"135\", \"Not-A.Brand\";v=\"8\", \"Chromium\";v=\"135\"",
            "sec-ch-ua-arch": "\"x86\"",
            "sec-ch-ua-bitness": "\"64\"",
            "sec-ch-ua-full-version": "\"135.0.3179.73\"",
            "sec-ch-ua-full-version-list": "\"Microsoft Edge\";v=\"135.0.3179.73\", \"Not-A.Brand\";v=\"8.0.0.0\", \"Chromium\";v=\"135.0.7049.85\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-model": "\"\"",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-ch-ua-platform-version": "\"10.0.0\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
        }
        request.cookies = {
    "_lr_env_src_ats": "false",
    "hb_insticator_uid": "ac76c294-0ee6-443a-aada-f21e36133234",
    "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
    "_hjSessionUser_2519920": "eyJpZCI6ImMyN2NkNWVkLTFhYmItNWQxOS1iYWEzLWIyNjQzOTJlMjgyNSIsImNyZWF0ZWQiOjE3NDI1NDgyODQ3NjcsImV4aXN0aW5nIjp0cnVlfQ==",
    "advanced_ads_visitor": "%7B%22browser_width%22%3A1857%7D",
    "gcid_first": "189ab6c1-d0d8-4cc5-867b-55edc0221d0e",
    "cto_bundle": "7odxRl9PT1JQUkJIcFlMNFJhcloxd0pnNGdmcTNVcGFTRFVuMXU5Rkk0VWtJcUlLJTJGaUFIN0J4NjMwSTI0N1Z0Rks5R2NwVUtpS1NTcGc4YkQzbkRMaUVGeHR1eTE3SVdhS085JTJCOFRiMWlVVCUyQm5yMUJnWURVVHhqQkU4NjZsMDlObWpTeUdKS2JtcVVHVmhiU3JldW5QWmRFUDdySXBKcDNOQlR6TmdhcVBzWGVVJTJCVSUzRA",
    "wp-wpml_current_language": "es",
    "_gid": "GA1.2.991146391.1744601747",
    "panoramaId_expiry": "1745206493417",
    "panoramaId": "c254b407f9df76bd201fd1b209d116d539385cadd59156c5fc78cb8587015291",
    "panoramaIdType": "panoIndiv",
    "_lr_retry_request": "true",
    "gc_session_id": "cb3jwufemo53lyt7ptao45",
    "_hjSession_2519920": "eyJpZCI6IjQ0MmYxYjUzLWM3MmMtNGMzZS05NTU3LTBiMzM2OTk2N2I0OSIsImMiOjE3NDQ2MDc0Mjk1MDcsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MX0=",
    "_gat_UA-13285749-1": "1",
    "cf_clearance": "gFa6Yy997ZKcALwmRqkIoGJg8myaADoFuUxhPjzAaLs-1744609229-1.2.1.1-zf8.6jYQJq1fHiorUsrx7eNZmwM.xtZLIehM9y0olfW3HxjttsZ8TEtQY6TtIU08xOk9ja6TPdkbcfwss7WYTkNuDke7GaFGy3Kw4P7NGhdcrLhmfYo_ZGnTaVaaxlFOuBaJYARj.3kHOV4Nq5jdjPpCXG0njQxTROtf4ID6YV7XU8crGariuPAdkG0EDr5i3VRkIJ6ujlpcpGTPWLQaS3dG5r0cXSu5PdrjGh.p_ybiutghiiRker0N6ljtM6K0nx6Gkm2lg7X_ClMaMAuD3uBEaZY348eIbZ8xdTzj.cK5faJNZCcEjuXjjvI0FECE9stawj7f9D2QDkxaObRnBAlGb1Ppy.oqDcFdEOnQkNwIieAF7VA_me8aBJGVJxJu",
    "_ga_C49PKYGF8J": "GS1.1.1744609285.9.0.1744609285.60.0.0",
    "_ga_QXPK198T2V": "GS1.1.1744609285.9.0.1744609285.60.0.0",
    "_tfpvi": "MGNjYWM5YmEtYzk2Yi00ZWJjLTljYzgtMTZlZDI2YmQxMDg4IzAtMg%3D%3D",
    "_ga_D9VYDM6HXL": "GS1.1.1744609286.6.0.1744609286.60.0.1965991891",
    "_ga": "GA1.2.1794378842.1742548245"
}
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = list(set(response.xpath("//article//h2/a/@href").extract()))

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
            items.table_name = self.table  # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url = item
            # items.title = item.get("fields").get("Titolo")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://confidencial.digital/page/{current_page}/"
        params = {
            "s": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, method='GET',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(
            response.xpath("//div[@class='content']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
