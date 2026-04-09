import json

import feapder
from feapder import Item
from lxml import etree
import re

from feapder.db.mysqldb import MysqlDB

class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.101.200", port=3307, db="spider_keywords", user_name="czm", user_pass="root"
    )
    sql = """ select db_name from keywords where country='Portugal' and language='葡萄牙语'"""
    mysql_db = db.find(sql, to_json=True)[0].get("db_name")
    print("待写入的数据库是:", mysql_db)# 判断数据库是否存在
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

    country = 'Portugal'
    table = 'Portugal'
    #葡萄牙语
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
    keywords = db.find(f"select keywords_list from keywords where language = '葡萄牙语' and country='{table}'", to_json=True)[0].get(
        "keywords_list")
    print("待抓取的关键词列===========>", keywords)
    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://www.cmjornal.pt/Pesquisa/LoadMorePesquisa"
            params = {
                "Query": f"{keyword}",
                "FirstPosition": "0",
                "Sort": "Relevance",
                "RangeType": "All",
                "From": "01/01/2000 00:00:00",
                "To": "28/03/2025 23:59:59"
            }
            data = {
                "X-Requested-With": "XMLHttpRequest"
            }
            yield feapder.Request(url, params=params, data=data, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://www.cmjornal.pt",
            "Referer": "https://www.cmjornal.pt/Pesquisa?SearchRequest.RangeType=All&SearchRequest.Sort=Relevance&SearchRequest.FirstPosition=0&SearchRequest.LastPosition=12&SearchRequest.ContentType=All&SearchRequest.Query=+chuva+intensa&SearchRequest.FromStr=21-03-2025&SearchRequest.ToStr=28-03-2025",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
            "X-Requested-With": "XMLHttpRequest",
            "sec-ch-ua": "\"Chromium\";v=\"134\", \"Not:A-Brand\";v=\"24\", \"Microsoft Edge\";v=\"134\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        request.cookies = {
            "cof_appid2024": "ce1eeae316eb47bf95c7c36fb85c05d7f62eb2d224ad44bab61e419c8c2822e9",
            "_gcl_au": "1.1.1316146267.1739762713",
            "_ga": "GA1.1.1350194515.1739762713",
            "_fbp": "fb.1.1739762717776.998391492297005189",
            "gig_bootstrap_3_YSlY5IMn5vYKp-89zA5ZxwfsMCoXJcIhfkTQku0j7e6ZExbl1qaT4jhDKiIMTKqb": "_gigya_ver4",
            "connectId": "{\"ttl\":86400000,\"lastUsed\":1739762773905,\"lastSynced\":1739762773905}",
            "cto_bundle": "rWzWTl9PT1JQUkJIcFlMNFJhcloxd0pnNGdSNjN2MnI3eHBjMmxRQlJ6cXNZN2txZHE4S0hzakVSNjN3cSUyQnloSkJ6UGxveWRaUGZKNjZjSjlQdzVFTGxvbHFYNUd4YjhWQXlqWlROV0FET2VRaVoyYVMzQ05ENHlSamhlVnZKaEp6V1ZQJTJCQm9UZGZuQ25ZNCUyRjVkbFR0T01wSUElM0QlM0Q",
            "_cb": "DZC-uvBrKvOKSjUqS",
            ".AspNetCore.Antiforgery.lqape9LhXME": "CfDJ8EDyBCgqeVdGkCEdfKxPbRmLUXKr5OR7yagJ25q8rx2APKDt3bQ2D_hyrcq5fbGTEiwEtnpzpZCcjY1dj9wt0B1ls5i2k22MXRBu6sdR8A_Jq9Q4zq3My_6GaW3-OZOn4bR4hCKGhX9qXGtKPXHZpyE",
            "__gfp_64b": "Cwm64b4KvtPWOz1P0h96qStsGr6sxOa6W66VQkbSLLD.J7|1740638158|2|||8:1:80",
            "_cb_svref": "external",
            "PushWarning": "true",
            "___iat_ses": "9E331BB55D277905",
            "___iat_vis": "9E331BB55D277905.0db72a8194adde84a9b0bf0a45ebfa4f.1743123911454.db86a8c6d7d33cc9d4c5e41dd63165e1.UBEMJZMAUA.11111111.1-0.c39abd82f3c462d23f9dd182faaacad1",
            "FCNEC": "%5B%5B%22AKsRol_DsvSYLh2Aht0DZIh3TI4hfN-7DbK9-foOV4z0h6HIPc_EZCEBzUqfpYJxWrdis-GePdp3SSuTWQctQVw-6ShINWQj3UCL4LQyq1dOARdImurozXcA_WoriOGuuoZr0KkuzMkL2y26QEWdj5otnCSXUanBzQ%3D%3D%22%5D%5D",
            "_chartbeat2": ".1740638209353.1743123959353.0000000000000001.Ch4yV7CvChWQDIbOVgDhafhLCBxIUQ.4",
            "_ga_4KCFYMNHZC": "GS1.1.1743123804.3.1.1743123972.44.0.0",
            "_chartbeat5": "659|4473|%2FPesquisa|https%3A%2F%2Fwww.cmjornal.pt%2FPesquisa%2FLoadMorePesquisa%3FQuery%3D%2520chuva%2520intensa%26FirstPosition%3D12%26Sort%3DRelevance%26RangeType%3DAll%26From%3D28%252F03%252F2015%252000%253A00%253A00%26To%3D28%252F03%252F2025%252023%253A59%253A59|CcdE3dDeP6q_ZPt6mBgyosWBUc6-p||c|D2DQQpyGdr_fIA6BC2IbQ-C1QjFf|cmjornal.xl.pt|"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//article//div[@class='text_container']/a/@href").extract()

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
            if "videos" in item:
                continue
            items.table_name = self.table
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 12
        url = "https://www.cmjornal.pt/Pesquisa/LoadMorePesquisa"
        params = {
            "Query": f"{current_keyword}",
            "FirstPosition": f"{current_page}",
            "Sort": "Relevance",
            "RangeType": "All",
            "From": "01/01/2000 00:00:00",
            "To": "28/03/2025 23:59:59"
        }
        data = {
            "X-Requested-With": "XMLHttpRequest"
        }
        yield feapder.Request(url, data=data,params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//section[@class='corpo_noticia']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
