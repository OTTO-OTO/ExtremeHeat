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
    sql = """ select db_name from keywords where country='Austria' and language='德语'"""
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

    country = 'Austria'
    table = 'Austria'
    #德语
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
    keywords = db.find(f"select keywords_list from keywords where language = '德语' and country='{table}'", to_json=True)[0].get(
        "keywords_list")
    print("待抓取的关键词列===========>", keywords)
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.diepresse.com/suche"
            params = {
                "s": f"{keyword}",
                "p": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, method='GET',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.diepresse.com/suche?s=extreme+Temperatur",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0"
        }
        request.cookies = {
            "didomi_token": "eyJ1c2VyX2lkIjoiMTk0ZWQ3MzMtYWU5YS02YzdhLTkzMjQtNGJjZmYzMjQzNzU3IiwiY3JlYXRlZCI6IjIwMjUtMDItMTBUMDE6MjI6MTkuNDk3WiIsInVwZGF0ZWQiOiIyMDI1LTAyLTEwVDAxOjIyOjIwLjk1NVoiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwidHdpdHRlciIsImM6cGlhbm9zb2Z0LUtGQ2h6Nm1yIiwiYzpzcG90aWZ5LWVtYmVkIiwiYzpodWJzcG90IiwiYzpiaW5nLWFkcyIsImM6eW91dHViZSIsImM6Y2hhcnRiZWF0IiwiYzptYXJpbi1zb2Z0d2FyZSIsImM6aW50ZWxsaWFkIiwiYzpzdW5kYXlza3kiLCJjOmFrYW1haS1jb29raWUtc3luYyIsImM6dXNhYmlsbGEiLCJjOnJlZmluZWQtbGFicyIsImM6c3luYWNvciIsImM6dGlrdG9rLUtaQVVRTFo5IiwiYzpnb29nbGVhbmEtNFRYbkppZ1IiLCJjOnBvd2VybGluay1BM0xlRE1GNCIsImM6YXBwbGllZHRlLWRhWlEyQVlUIiwiYzppbnN0YWdyYW0iLCJjOmtvY2hhdmFpbi1OQVRDOFoyYSIsImM6c2ltcGxlY2FzdC1BQkg4UEVwSiIsImM6cGludGVyZXN0IiwiYzpkaWVwcmVzc2UtcFpnYUYzVUUiLCJjOnN0b3J5Z2l6ZS1DNmVja1FHWSIsImM6YXBhdmlkZW9wLVZkUm1xa0dyIiwiYzpmaXJlYmFzZS1UV01LR0NnRCIsImM6cGl3aWtwcm8tejRCYXpSZFYiLCJjOnNvdW5kY2xvdWQtV2h3cTdLZVkiLCJjOmFwYXdhaGx0by1lZmpVZGtpRSIsImM6YWRkZW5kdW0tQUV4NGhLOVoiLCJjOmFwcGxlbXVzaS1jUmNHVEVReiIsImM6YnJhZG1heC1qTkszVE5uaCIsImM6Y3RwdGVjaGwtVHQ5NzZkZmUiLCJjOmV2ZXJ2aXotVWZEN2duTlQiLCJjOmNoYW5uZWxmYS1BN0pUVDRLUCIsImM6Y2l0cml4c3lzLTJBNEV4ZWtRIiwiYzoyM2RlZ3JlZXMtNmpRWmZDN1AiLCJjOnJpZGRsZXRlYy1RUUtBTEo5RCIsImM6ZGF0YXdyYXBwZS1IUGFFOVZuVyIsImM6ZmxvdXJpc2hrLWRSbnplSmNnIiwiYzpnb29nbGVleHQtWldqQkFpTDgiLCJjOmdvb2dsZW1hcC1NVU5uQjlhciIsImM6Z29vZ2xlcmUtclA2RTJ6UHoiLCJjOmdvb2dsZXRhZy1RTVh5Y0xIRSIsImM6aW5mb2dyYW0tV2ZNQ0FnOFoiLCJjOmtleXRpbGVzYS1kR1o2N2dmNiIsImM6cGFnZWZsb3ctM05jWGo3Y3giLCJjOnBvY2tldG1hdGgtM25mZm5pSG0iLCJjOnBvZGlnZWVnbS1GYXJxTVI2bSIsImM6cm9ja2NvbnRlLTdtV2FBdFRmIiwiYzpzY3JpYmJsZWxpLU5pZXpFeWVpIiwiYzpzb3VyY2VmYWItTkRta0VjcTkiLCJjOnRob3JtZWRpYS1DcGZKRjRUMiIsImM6dGltZWxpbmVqLXpqMkRZVkM4IiwiYzp1YmltZXRnbWItQlZNV3FoYlgiLCJjOmdhcGZpc2gtSzNYZzY5SzIiLCJjOmdza2lubmVyLVZobjJQekFFIl19LCJwdXJwb3NlcyI6eyJlbmFibGVkIjpbImRldmljZV9jaGFyYWN0ZXJpc3RpY3MiLCJnZW9sb2NhdGlvbl9kYXRhIiwic29jaWFsbWVkaWEiLCJmdW5rdGlvbmFsIiwiMjciXX0sInZlcnNpb24iOjIsImFjIjoiREFXQWdBRVlBTElBandCbEFFM0FacUJFa0NYNEdEQU1SQWJXQV8tQ2hFRk9vS21nWUNnQS5BQUFBIn0=",
            "euconsent-v2": "CQMnI8AQMnI8AAHABBENBbFsAP_gAAAAAACYJjIR5C5UTWFgYHpzQZsEOA0f1kBIBsAABhKBAaABiDKAcIwCkGECAAQAACACAQAAoAABIABECAAAAEAAQIAABAAMAAAAAAAIIAAAAAEAAAIAAAAICAgAAQAIgEBFEhUAkQAAABKAxEAAAIAgAAQAAAABAAAAAAAABCAAAAAAAAAAQCgAEAIAAIIAAAAAAAAAgAAAAAEAAAAAABAAAAMOgAwABBWUpABgACCspCADAAEFZSUAGAAIKyloAMAAQVlCQAYAAgrK.f_wAAAAAAAAA",
            "fidcnst": "1",
            "_cb": "PI_6yDs-TYZCzRCOz",
            "_cb_svref": "external",
            "_gcl_au": "1.1.1600608341.1739150542",
            "_pctx": "%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAEzIEYOBWAZgCYAbAHYAnAA5B3QQAZhAwYpABfIA",
            "_pcid": "%7B%22browserId%22%3A%22m6ydbvfeqxx2thqg%22%7D",
            "_gid": "GA1.2.54097518.1739150543",
            "kt_uniqueWebClientId": "41bae17b-687a-40cf-96d1-e477e7f433f3",
            "_fbp": "fb.1.1739150544091.280266024798553833",
            "__pid": ".diepresse.com",
            "__pnahc": "0",
            "__pat": "3600000",
            "xbc": "%7Bkpcd%7DChBtNnlkYnZmZXF4eDJ0aHFnEgpERmpBdk12Qko1Gjw1bmFVczlGNEtPY2xTZVVISUNLU2pWVk9KcDR6bmxiTzBjaVFuaU5Ncjh4b0tUVVlrWmZ6RnlhMFFvcTYgAA",
            "cX_P": "m6ydbvfeqxx2thqg",
            "firstid": "114e7df6080549bbb42e85e7394c9b1d",
            "cX_G": "cx%3A2ch5836kgw0dk2o2o76x58ntq%3A3a7ubacqqrvkw",
            "trc_cookie_storage": "taboola%2520global%253Auser-id%3Dbc8d2acd-2d31-40ef-a034-ba854d4d481c-tuctea2da44",
            "_dc_gtm_UA-98290434-1": "1",
            "_dc_gtm_UA-57194112-1": "1",
            "ioam2018": "0002b835cb181b1da67a954ba%3A1770168143271%3A1739150543271%3A.diepresse.com%3A4%3Aat_w_comdiepres%3ARedCont%2FHomepage%2FHomepage%3Anoevent%3A1739150624969%3A6glddp",
            "_ga": "GA1.2.894632235.1739150543",
            "__pvi": "eyJpZCI6InYtbTZ5ZGJ2ZnFoNWRwbHI3MiIsImRvbWFpbiI6Ii5kaWVwcmVzc2UuY29tIiwidGltZSI6MTczOTE1MDYyNTQ5NX0%3D",
            "__adblocker": "false",
            "_ga_6DB54PXRZS": "GS1.2.1739150543.1.1.1739150625.45.0.0",
            "cto_bundle": "mdge-19PT1JQUkJIcFlMNFJhcloxd0pnNGdlT01lWHdjQzhhVzdRQUNqRTRrSkRseUJ4MVpaVWVwbSUyRlVRc0ZhY0s4QmtTbXlRbVBOdDlIOTc0OHl6NlhnTWZ5c25yZndLNzh3QWozaW5ydlBnZGJzZW95WTAlMkZPOUN0Sm0lMkJ3VDQ5THZzZnFxclZ6Q0J3cEhMaVY3QlNtYzElMkI4ZyUzRCUzRA",
            "cto_bidid": "FW5B2193cGh4NjNXS2RuekNWYSUyQnNNSGI2b2tYWmEwdEg5S1JVOHFHZzh4aUYxM01EaWdTYWY0NnR2JTJCUU0wOTJSSU9KUSUyQkh5RnBmOGJrYjlxeiUyQmN2QlluNSUyRmdheUZQem1GOGRqc2hpb3JYRWRTbTglM0Q",
            "__tbc": "%7Bkpcd%7DChBtNnlkYnZmZXF4eDJ0aHFnEgpERmpBdk12Qko1Gjw1bmFVczlGNEtPY2xTZVVISUNLU2pWVk9KcDR6bmxiTzBjaVFuaU5Ncjh4b0tUVVlrWmZ6RnlhMFFvcTYgAA",
            "_pcus": "eyJ1c2VyU2VnbWVudHMiOnsiQ09NUE9TRVIxWCI6eyJzZWdtZW50cyI6WyJMVHM6ZDYzMjYxZGFhN2UxMjFkNDg4YjhlZDIzYWM5ZWI0YzljMGU1ZGY1ODozIiwiTFRyZWc6OWU0MTkzY2U5NjQ5YjY3NTVlZDdmOGZjOTk2YmMxZjQyNzUwMzkyNTowIiwiTFRyZXR1cm46MTMwZmEzZmRlOTFiZTQ1OWMzMjk4YzA4Zjc3M2QyNGJiY2RkMDk2ZjoxIiwiTFRjOmMyZTlmY2IzMzU5MDNmNTUyNGEwMzk1ZThkYWUwMmMwOGE0ZmQ5OTc6bm9fc2NvcmUiXX19fQ%3D%3D",
            "cto_dna_bundle": "1-Ked19PT1JQUkJIcFlMNFJhcloxd0pnNGdSZjV4Y3BGUGg5dk5mZU01djluS0glMkJKUzQ4QmYzM2xMTm9rVk9LbEVQaHNub2Z2ak9EdnklMkYzaEJkalhmaEV4MFElM0QlM0Q",
            "_chartbeat2": ".1739150541741.1739150628060.1.B-yvYzdl1s3CpFSikDIDJMhC0L_xq.4",
            "_ga_7860QQTVF9": "GS1.1.1739150542.1.1.1739150633.32.0.0",
            "_chartbeat5": "962|2908|%2Fsuche|https%3A%2F%2Fwww.diepresse.com%2Fsuche%3Fs%3Dextreme%2520Temperatur%26p%3D2|BkE9vjBTzAuZCR5UdlB9Pn6-hedKF||c|BkE9vjBTzAuZCR5UdlB9Pn6-hedKF|diepresse.com|"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        # print(type(response.text))
        # print(type(response.json))
        # links = json.loads(response.text)
        # print(response.text)
        # print(response.text)
        # print(response)
        links = response.xpath("//a[@class='card__link']/@href").extract()
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
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.diepresse.com/suche"
        params = {
            "s": f"{current_keyword}",
            "p": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, method='GET',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@id='article-body']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
