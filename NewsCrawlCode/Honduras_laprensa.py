import json
import re

import feapder
from feapder import Item
from curl_cffi import requests
from lxml import etree
from feapder.db.mysqldb import MysqlDB

class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.101.200", port=3307, db="spider_keywords", user_name="czm", user_pass="root"
    )
    sql = """ select db_name from keywords where country='Honduras' and language = '西班牙语'"""
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

    country = 'Honduras'
    table = 'Honduras'
    #西班牙语
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
    print("待抓取的关键词列===========>", keywords)
    print("待抓取的关键词的类型===========>", type(keywords))
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = f"https://www.laprensa.hn/busquedas/-/search/{keyword}/false/false/20000226/20250226/date/true/true/0/0/meta/0/0/0/1"
            yield feapder.Request(url, callback=self.parse_url, page=page,keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.laprensa.hn/busquedas/-/search/Calor%20/false/false/19821227/20241227/date/true/true/0/0/meta/0/0/0/2",
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
            "ITR_COOKIE_DEVID": "3c73c196fbcee06b16966f62ab1669d900",
            "ITR_COOKIE_USRID": "3c73c196fbcee06b16966f62ab1669d900",
            "ITERWEBGEO": "DLa1Ribf4Wu4oyr1SEDocdkRIv3KuJVjIesm4S0rOeDxIEe1xQwfau0SFE0IoM5bfpm6gutL+a/nbtAObruITw==",
            "___nrbi": "%7B%22firstVisit%22%3A1735281301%2C%22userId%22%3A%22baf57cbb-f11d-44fa-b0b5-d404d3412808%22%2C%22userVars%22%3A%5B%5D%2C%22futurePreviousVisit%22%3A1735281301%2C%22timesVisited%22%3A1%7D",
            "compass_uid": "baf57cbb-f11d-44fa-b0b5-d404d3412808",
            "permutive-id": "daf20e76-7782-4725-ad01-647d7b0ecbf3",
            "ev_sid": "676e4a98f28ea56ac55b92b9",
            "ev_did": "676e4a98f28ea56ac55b92b8",
            "_sp_ses.5d02": "*",
            "_matheriSegs": "MATHER_U1_PAGEVIEW5_20230623",
            "_matherSegments": "MATHER_U1_PAGEVIEW5_20230623",
            "_gid": "GA1.2.1141012228.1735281311",
            "_pk_ses.3A8O0VOKF0YQQ.4900": "1",
            "_fbp": "fb.1.1735281313581.70904741972668089",
            "_clck": "1dkaiqd%7C2%7Cfs2%7C0%7C1822",
            "_pk_ref.3A8O0VOKF0YQQ.4900": "%5B%22%22%2C%22%22%2C0%2C%22%22%5D",
            "___nrbic": "%7B%22isNewUser%22%3Atrue%2C%22previousVisit%22%3A1735281301%2C%22currentVisitStarted%22%3A1735281301%2C%22sessionId%22%3A%22a0229f70-b991-41c6-980c-3c7904437355%22%2C%22sessionVars%22%3A%5B%5D%2C%22visitedInThisSession%22%3Atrue%2C%22pagesViewed%22%3A3%2C%22landingPage%22%3A%22https%3A//www.laprensa.hn/%22%2C%22referrer%22%3A%22%22%2C%22lpti%22%3Anull%7D",
            "_ga": "GA1.2.27966610.1735281311",
            "_gat_UA-45957560-5": "1",
            "_clsk": "rqdrr9%7C1735281394173%7C3%7C1%7Cf.clarity.ms%2Fcollect",
            "_ga_W4001FDHDG": "GS1.1.1735281310.1.1.1735281396.39.0.0",
            "_pk_id.3A8O0VOKF0YQQ.4900": "ba07e5288ff3291b.1735281313.1.1735281397.1735281313.",
            "_sp_id.5d02": "ff27be63-d7a5-46de-8293-eac3e01d8068.1735281311.1.1735281402.1735281311"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//div[@class='card-title title']/a/@href").extract()
        current_page = request.page

        current_links = links
        if self.previous_links is not None and current_links == self.previous_links:
            print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            print(item)
            items = Item()
            items.table_name = self.table
            items.article_url = item
            items.country = self.country
            items.keyword = request.keyword # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://www.laprensa.hn/busquedas/-/search/{current_keyword}/false/false/20000226/20250226/date/true/true/0/0/meta/0/0/0/{current_page}"
        yield feapder.Request(url,callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        # print(response.text)
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='paragraph']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='date-published']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items

if __name__ == "__main__":
    AirSpiderDemo().start()
    s= ["Tormenta tropical 'Earl'", "Depresión tropical '16'/Huracán 'Nate'", "Tormenta tropical Amanda", "Huracán Eta", "Huracán Iota", "Huracán Julia", "Tormenta tropical Pilar", "Tormenta tropical Alberto", "Ciclón 'Sara'"]
