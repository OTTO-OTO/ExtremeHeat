# -*- coding: utf-8 -*-
"""

有加密参数

"""
import re

import feapder
from feapder import Item
import json
from lxml import etree

from feapder.db.mysqldb import MysqlDB


class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.42.97", port=3306, db="spider_data", user_name="czm", user_pass="root"
    )
    # 创建表结构
    table = 'United_States'
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

        SPIDER_THREAD_COUNT=3,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[8, 10],
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
    
    def __init__(self):
        super().__init__()
        self.previous_token = {}  # 每个关键词独立的token跟踪

    country = 'United States'
    # 英语关键词
    keywords = ["Extreme", "Heat", "High", "Temperature", "Heavy", "Rain", "Drought", "Power", "Outage", "from", "Fire", "Air", "Pollution", "Climate", "Change", "Crop", "Yield", "Reduction", "Oxygen", "Deficiency", "Affecting", "Traffic", "Ecological", "Disaster", "Economy", "Marine", "Heatwave", "Coral"]
    print("待抓取的关键词列===========>", keywords)

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.washingtonpost.com/search/api/search/"
            data = {
                "searchTerm": f"{keyword}",
                "filters": {
                    "sortBy": "relevancy",
                    "dateRestrict": "",
                    "start": 0,
                    "author": "",
                    "section": "",
                    "nextPageToken": ""
                }
            }
            data = json.dumps(data, separators=(',', ':'))
            yield feapder.Request(url, data=data, callback=self.parse_url,keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "content-type": "application/json",
            "origin": "https://www.washingtonpost.com",
            "priority": "u=1, i",
            "referer": "https://www.washingtonpost.com/search/?query=heatwave",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "wp_ak_wab": "0|0|1|1|1|1|1|0|1|20230418",
            "wp_usp": "1---",
            "wp_ak_bt": "1|20200518",
            "wp_ak_bfd": "1|20201222",
            "wp_ak_tos": "1|20211110",
            "wp_ak_sff": "1|20220425",
            "wp_ak_lr": "0|20221020",
            "wp_ak_co": "2|20220505",
            "wp_ak_btap": "1|20211118",
            "_cb": "D1sFf3CZb_v6DqHgME",
            "_ga": "GA1.1.1489517971.1734772547",
            "wp_ak_kywrd_ab": "1",
            "wp_ak_v_mab": "0|0|3|1|20250110",
            "_v__chartbeat3": "CbEHiBBG8kYmDLgYXg",
            "wp_ak_subs": "1|20250113",
            "sec_wapo_login_id": "e0d40fce-91fd-49ac-95c6-efc879b1205b",
            "wapo_display": "2687833212",
            "wapo_groups": "default-onestep-short",
            "wapo_login_id": "e0d40fce-91fd-49ac-95c6-efc879b1205b",
            "wapo_provider": "Washington%20Post",
            "wp_ttrid": "\"c9bfbf8d-c0aa-4923-82b0-1e81b18279f8.1\"",
            "wp_devicetype": "0",
            "wp_ak_signinv2": "1|20230125",
            "wp_geo": "SG||||INTL",
            "_t_tests": "eyJqeXVpZVhZdVNrMFc4Ijp7ImNob3NlblZhcmlhbnQiOiJEIiwic3BlY2lmaWNMb2NhdGlvbiI6WyJCSTNOUEwiXX0sIm9YSkhXWFU5ZUk1eTIiOnsiY2hvc2VuVmFyaWFudCI6IkIiLCJzcGVjaWZpY0xvY2F0aW9uIjpbIkRhblRndCJdfSwibGlmdF9leHAiOiJtIn0=",
            "wp_ak_pp": "1|20210310",
            "sec_wapo_secure_login_id": "WAPO-z9RuV1%2BU140%2FQjBa5oRFBTWnym4%2FKSEUTasqSrMNeulHBKzjwH1qswd21enfOJ5qhuT%2Bp8xLMgYYpbmB5XL42nkfpNr7gcekq6tp%2F1crt2VWPgAo%2FOOvosXrWKVbipfx39jyryMwOSBzdy7KPHMuOu3QINO8t6iJY%2FnND3iICYRJAuK%2Bs7zwJaZe4xGifpibCLgGhuHL%2FR6LQR%2Bfavlpb2ZhP0QldHYj3EHNvN999yynYDxvKzQfRr3e4Ei3FLn6Oa8KHo7v26VZe6N%2F6hc0eJ8gVtbvF4pYcAQphT2zG7k%3D",
            "wapo_actmgmt": "v:3|i:F0DF8C085C41F3317186E29CB6317CBF|isub:0|SC:0|ts:1737440090125",
            "wapo_secure_login_id": "WAPO-FyGqrVgAVq4sQqIGECDGwrWjILOimswG1U5noWnK8k48jQu3YRY%2F4LZiRPzDv%2FtjXAN2QWzeIgU0JafDusNIgbvbAmdT%2FBjPhTj%2FjbUq9B4cuO98S%2B5fJ3GQrRSN8eAC2B1gBCHtOduf86Qhfgz5360dwYySoscA98lGPAccMrfDiZPN6UxNn1b3wXfueMCQRYVC6lyqxD7A0ZfohXpEfaphG%2FhHukSGql%2BUVtPKwNtMF7fihFpwcGuNgoSTBsV9iPPawXmV7h7wZcwV%2F3XDC1mtWOGbS0yiHs2kpgxfklo%3D",
            "wp_pwapi_ar": "\"H4sIAAAAAAAA/03LsRGAMAgF0F2oLSSfg5BtRMIEdrnsrqX1u7dIakaylwRMrEU3LXAWItCExqKHBrF9Kica0EH7IGZkZ40LMBXMaZHu6ZKpd/s3Vtipbkp7v6bgFL9vAAAA\"",
            "jctr_sid": "72450173744042031990179",
            "_fbp": "fb.1.1737440420882.1516315939",
            "jts-rw": "{\"u\":\"47182173477254752575273\"}",
            "cto_bundle": "9ppVMV9PT1JQUkJIcFlMNFJhcloxd0pnNGdZclZnTzVnU25RM2xsQmF4USUyRkFJUDE2NDE4bUluM1NjJTJCYTFKMEhLVXQ5eW1KOUFxeEN3QkxTVHg5VUNsTEg4NUVGOFBYSXN1YTNSY3NuR1JaSjZONW92SDcwRHgyYUZuY0d1MmNXemVTeGNCRG5IcVdsN1ZtbnoyVFdLSTkxOHpSY2VQTmttJTJGbXhVSTVETnZieTRKJTJCQSUzRA",
            "rpisb": "rBEAA2ePPJag1QAqQTWyAg==",
            "_abck": "E6216FCF202660DB9A83191040F15530~-1~YAAQFtYsF7mp/22UAQAAxKqEhw3fqPelJU9FfL46Sw7yhXznCYcTkwn6mncN2arl+kxKnVSjZ0FttH4K9VT/B5w4SujiMMUw0uBGJrVOYeMdmjMzyV71cifmDCFyZvcVuCsI/6AMop8Oqlhg1o89SB4I/O+x2R1gHt1QFwQ0wQxbjTQshW/y8pdD6ZZ+cV2JOHQiP0fXowbmRN/EMZId6EIpCCuZvoStIISo82Y6/JNLHTHgJuZ7OOE2kmpTKmJbeHptHqGJwROv/ztyjVaBbb0UhWHiJQeef9PxHBUwpJgTziDXgtKkumGzjV2GwrKc7jqk0evN5WN5aceovHO/ok85/nENNXyLoisNe6vT5FT0NDJng+X8hz137KO5G1B155e4JN/XubSrcU5iMtEdaxp1qKvwCntgqENdLK6LUQHdH4JDVPE=~-1~-1~-1",
            "bm_sz": "C093543E8FC57A49CA2C11F135FAF82B~YAAQFtYsF7up/22UAQAAxKqEhxqUfekxOmy5us07Y+DzbJ95x5GdzMpoZqhBro6HJFzDQPIvcKl4/Lo8bNXeM+ors0XzFxSW3twZ6+XR5P/jlUbjig/n+S85zgF3vgOXtBY3DLw3AXPnuEKr0LHwqL6K2Ew4FWYFyIqWrdzyEp9tovgNhE3ZisV5jTUMqqJ3BBSplXT4zxucIE9GYo1HNKtCFuozEsfn3QRmxSyr3orlYGrCltq8HQmaiWPeiQR0TxszCif1op5YFT9WFNG97UvMHhiJh5c2LsVuQpV/4m/yAC8jrwyRb40zWIPO93+jh/dkz3K4mxAytaLMIayseQFh9vFHWQL/H1GZtvnBsTIVDXRat47oClTAosai8AwL~4274497~3683649",
            "jts-fbp": "fb.1.1737440420882.1516315939",
            "bm_mi": "252F6DF78BD7B2347A7C38B94001B512~YAAQFtYsF6ep/22UAQAAz6mEhxrZfzlZfCs48eZwHf/dyDR3gQERGHe3nSnLVwlleiPuakiZ+ccGELTm73HfYB2yvhwc1p/CRUiNwvDY1NVG21oQx6xJVFbkJfpztBGwdbbYpRKfFLWw5V3d00A40zNJ7hpYA9CoNT+lZAijDyFVWZiCRIMPKd4hXlxN0JVa9YPMJpz1WHbMdWMXaSG5j7IPgogVwOMcsMFn2Tz+PentSiV7vDPB01v9toW3+ZDVj3tbjFJGwekuxiRkrQmiZngcZmzKvETqQ1l6+ofu7UIYr+NkoTCZzle53PPuy1dxszEVsp9wtAnlESMM+LJ0HjbslnF+m7Gdnpay72jBbkyGCcRNgR+lnNyDrKQsWK4PPs49LLwxZ8HTpDjE~1",
            "ak_bmsc": "DBE793795E5CA5217E427139B78B8F2D~000000000000000000000000000000~YAAQKto4fWFXo22UAQAAlOSEhxrJqp718c+ps7LpshG40vLcTUm9G3OgXOICGZ8sVsxJcg29NgreWQFFm1F879T+le21IWR/foNIt3/UGz7A/+YBb8407O2hfbQB9Ig2RfdnYlbdnbSAtMPOysWU/TyWnoXmtD7ghoIFUVgRQNf+gJh82yegCQID7NiuJFXw9zeZcAcTZaC+LWKXcu5pygK2rBn4A1TskYEipJMdW68+S1dqHeqXBnPIgLu/s0cmAC/IcYtBiJ9W9qiUu7mOru5HzgCjXwTmpja/y2dJOmD4tJJwBe7g9t4YdHA9+9cbeOP1iIbaRCZdjO2VAEMnRc2oZvDqe1fmuP8H1NDWDyqMxHIyL+KdmTVDn61TJSw3xAbFA+U41vuxrw8PrsDtvXouLCAkUFN61klFAeQdyrkXSO/ZF3Gfv386KXYs0Bd2ANFr9Z3cv0F3TG1bLli0tvpn6CeY8N1l9pJbrI+Daw6VoYrux6zyA60f",
            "_chartbeat2": ".1734772546394.1737440439179.0000000000011001.CPTxBBBN-6c6CFsqdiBPAXwF3QuQr.1",
            "_cb_svref": "external",
            "permutive-id": "13011c44-e6d2-4116-8e2a-3412f4f2cfba",
            "_chartbeat4": "t=DRs0ToBnYvZ3CjnlD8DE8yZX2aQLK&E=16&x=5360&c=2.92&y=8925&w=323",
            "_ga_WRCN68Y2LD": "GS1.1.1737440186.6.1.1737440480.0.0.0",
            "wp_s": "T.1.1737440104.1737440480.2.1.0.1",
            "bm_sv": "C8ADB3944272CB362BB1839ACC3CFD0A~YAAQFtYsF1q0/22UAQAAH5SFhxrF8mpXmi8x6wwokZ5XXMqrxpQBYQIZxtfU6Ci8+YY5DpB6F9+XNA2u4D1XeIcJ3NChBRzDKNIWsmb8tv+0670xYsKY7kOYszkAQGTYohyftU6fuIVkLguDTPiHSOMsf2oyQKP4CJyIDDki2PO76j7l2aoK7tOsjj2sV75d1NhIgJgOnPrWfb0kGW69sWFM8Rq+CmbBXHST8Sng6tD/Bnglnmt5TTg6LZMOSVCerDTZTKbZsJ7C~1"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        links = response.json['body'].get("items")
        next_token = response.json['body'].get("searchInformation").get("nextPageToken")

        if current_keyword in self.previous_token and self.previous_token[current_keyword] == next_token:
            print(f"关键词 '{current_keyword}' 的下一页没有新内容，退出当前关键词的循环。")
            return None

        self.previous_token[current_keyword] = next_token  # 更新上一页的token


        if not links:
            print(f"关键词 {current_keyword} 没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            items = Item()
            items.table_name = self.table # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url = item.get("link")
            items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        if next_token:
            url = "https://www.washingtonpost.com/search/api/search/"
            data = {
                "searchTerm": f"{current_keyword}",
                "filters": {
                    "sortBy": "relevancy",
                    "dateRestrict": "",
                    "start": 0,
                    "author": "",
                    "section": "",
                    "nextPageToken": f"{next_token}"
                }
            }
            data = json.dumps(data, separators=(',', ':'))
            yield feapder.Request(url, data=data, callback=self.parse_url,
                                  keyword=current_keyword,
                                  filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        # items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//article//div//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
