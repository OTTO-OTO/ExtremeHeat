# -*- coding: utf-8 -*-

import feapder
from feapder import Item
from curl_cffi import requests
from lxml import etree
from feapder.db.mysqldb import MysqlDB


class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.42.183", port=3306, db="spider_data", user_name="czm", user_pass="root"
    )
    # 创建表结构
    table = 'France_2'
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

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[8, 10],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.42.183",
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
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )

    country = 'France'
    # 英文和法文关键词
    keywords = [ 
        "Extreme", "Extrême", 
        "Heat", "Chaleur", 
        "High Temperature", "Température élevée", 
        "Heavy Rain", "Fortes pluies", 
        "Drought", "Sécheresse", 
        "Power Outage from Heat", "Panne d'électricité due à la chaleur", 
        "Fire", "Incendie", 
        "Air Pollution", "Pollution de l'air", 
        "Climate Change", "Changement climatique", 
        "Crop Yield Reduction", "Réduction des rendements agricoles", 
        "Oxygen Deficiency", "Hypoxie", 
        "Oxygen Deficiency", "Hypoxie", 
        "High Temperature Affecting Traffic", "Température élevée affectant la circulation", 
        "Ecological Disaster", "Catastrophe écologique", 
        "Climate Change Affecting Economy", "Changement climatique affectant l'économie", 
        "Marine Heatwave", "Vague de chaleur marine", 
        "High Temperature Pollution", "Pollution due à la haute température", 
        "Coral", "Corail"
    ]
    previous_links = None

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.lemonde.fr/recherche/"
            params = {
                "search_keywords": f"{keyword}",
                "start_at": "19/12/1944",
                "end_at": "23/01/2025",
                "search_sort": "relevance_desc",
                "page": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.lemonde.fr/recherche/?search_keywords=Impact+trafic&start_at=19%2F12%2F1944&end_at=23%2F01%2F2025&search_sort=relevance_desc",
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
            "_pctx": "%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAE0RXSwH18yBbANYAHAGwBzAByUAnAB9%2BAVgAMMejADGMkAF8gA",
            "lmd_pa": "1734941744",
            "atidx": "14B38DB2-FC3D-4CC0-98E4-5A6F110D7833",
            "atid": "14B38DB2-FC3D-4CC0-98E4-5A6F110D7833",
            "euconsent-v2": "CQKFpAAQKFpAAFzABBFRBTFsAP_gAEPgAAqII7MB7C7OQSFicX51AOsESY1ewkBSoEAgBAABgQgBABgAsIwRkGACIAHAAqAKAAIAImRBIQAtGAjABAAAQIAAASCEAECAABAAJKAAAEAQAAEACAgACQEAAAAAgEAAAAQAgAJAAEooQERAAgAgLAAAIAABAIAEAgAIAAAAAAAAAAAAAGAAAAAAAAAAAAAAAAAAEEBAARBQiIACwICQggDCCBACoIQAIAAAQAAAAwAAAAAwAUAYACDABAAAAAAAEAAAAARAAgAAAgAQgAAAAIEAAAAAAAAAAAAgAABAAAAAAAAAAAAQAAIAAAAAAAAAAAAIAQAAgAAAAICAAAAAQFgAAAAAAgEAAAAAAAAAAAAAAAAAAACAAA",
            "lmd_consent": "%7B%22userId%22%3A%221acefd04-f3f0-4df9-9a2f-56110bd19ea5%22%2C%22timestamp%22%3A%221734941746.169987654%22%2C%22version%22%3A1%2C%22cmpId%22%3A371%2C%22displayMode%22%3A%22cookiewall%22%2C%22purposes%22%3A%7B%22analytics%22%3Atrue%2C%22ads%22%3Atrue%2C%22personalization%22%3Atrue%2C%22mediaPlatforms%22%3Atrue%2C%22social%22%3Atrue%7D%7D",
            "pa_privacy": "%22optin%22",
            "atauthority": "%7B%22name%22%3A%22atauthority%22%2C%22val%22%3A%7B%22authority_name%22%3A%22default%22%2C%22visitor_mode%22%3A%22optin%22%7D%2C%22options%22%3A%7B%22end%22%3A%222026-01-24T08%3A15%3A49.586Z%22%2C%22path%22%3A%22%2F%22%7D%7D",
            "_cb": "BbGxbiBDVv4BCwucU6",
            "_gcl_au": "1.1.1416510005.1734941750",
            "_fbp": "fb.1.1734941751261.317743860223122101",
            "__spdt": "ba3f3765eaa446ba895b6a36f3947fef",
            "lead": "904063fd-9752-4c55-bc1c-2f5f03aea461",
            "lead_ads": "904063fd-9752-4c55-bc1c-2f5f03aea461",
            "_pin_unauth": "dWlkPU9XSm1Zek15TmpZdE5UVXlaaTAwTTJFMkxXRmhNakV0TVdGa056SXhOV1l4T1dGbQ",
            "atidvisitor": "%7B%22name%22%3A%22atidvisitor%22%2C%22val%22%3A%7B%22vrn%22%3A%22-43260-%22%7D%2C%22options%22%3A%7B%22path%22%3A%22%2F%22%2C%22session%22%3A15724800%2C%22end%22%3A15724800%7D%7D",
            "_cs_c": "1",
            "lmd_cap": "_7zb161to3",
            "cto_bundle": "v9DiRF9PT1JQUkJIcFlMNFJhcloxd0pnNGdiamlkSm5CWWtlUjRrVjBrSkRZaDdzRjJaRWpVVGQwYXptWTFIclFRWjdKUDhaM0pFVkxYUndyZEdWODZNWkpGN0xGV091MVRrak4lMkZUYk8yOElSYUJOR2NvWFRIcUQ0U3gwdXBXcTIxRVFkUHc3aHVEQUdTNXFmaGM5dWZuNjJvQSUzRCUzRA",
            "_cs_cvars": "%7B%222%22%3A%5B%22user_status%22%2C%22gratuit%22%5D%7D",
            "_cs_id": "090b7226-dac3-ae7e-cc0a-f547d3846ca3.1735266569.3.1737618319.1737618319.1730815733.1769430569199.1",
            "kw.session_ts": "1737622834840",
            "_sp_ses.fb3f": "*",
            "lmd_ab": "zw4%2Fzrw5HmQQuFx%2FLS1uzO4j6V0dUCcZigV%2Bc%2FGZ6HMwpg%2B9ez0AgVyjBRNubRRriT6G3xE09CXutvr2%2FszODNPWX2qgaOy8QVd3ernhFF%2F71dMUN97Y3GW9xu%2BscE%2BTLvk2TXv1u9OMn5U5gD%2BBCL%2Boev%2FjvonCS2uR525SVIX1jgTAeBQvw4BPgaiNGXnYIqTW%2B1SBzg4E0i8%3D",
            "lmd_edg": "2a5084fae9818c8dce2dcdc7af33aa1334b1294fbf41437f8402b6c8c747ba284fc5933896c0f073af67e5c3d40bb7552471d1c5033c255171217e3213e9f5bbffd936d7a6047b27e0a21af5e31bc8a4429151c91fd0243d2d0a00b3e50a4beea76ef68562698a3f0e290e038f3c32fb53879cb77f683dae83dcd9e471a61fd7fde7ba4d275d8622695f4b61065d630a398302172327aee74d9e9ff7f3a1ba091280f56b7649edfba5079f0f504c2a05498a9ed6c0712cb938124b1581bd78c72b4a3e64d026dd75d9b897f6545de8f24a68eddbf66cee0f52ca0e5e5864027d6212b6bebdd34fecbb1dc6948c9913dca2c6a5fa0439a5eef1e675f13d7127a45c0521ce327dfb6459ee711f4bcc6da0859ea0ec0fe42b747a08a17e357b2b8fcd106777a03e68d6e81b5897ac5bea97cf741e22b1d280887da611ebc8734a1f78df95a1295aec5aabfc7aa28bbd7c07cfac12884dd435a97ff5f99634a0ee85",
            "_cb_svref": "external",
            "_rdt_uuid": "1737623454962.85f9547d-081b-448f-be4b-24d23c5336c1",
            "_chartbeat2": ".1734941750298.1737623618497.0000000000000011.CVbzx9DPPZlvDuKDa7C2FgkOCXxQJu.6",
            "amplitude_id_7dec6d6e90d5288af6e9a882f8def199lemonde.fr": "eyJkZXZpY2VJZCI6IjIyNThiYjE1LTEyNTUtNGUyNC1hZmQwLWNhNDFjOTM3ZDk3YlIiLCJ1c2VySWQiOm51bGwsIm9wdE91dCI6ZmFsc2UsInNlc3Npb25JZCI6MTczNzYyMjgyNzUyOCwibGFzdEV2ZW50VGltZSI6MTczNzYyMzYxOTE2NywiZXZlbnRJZCI6NzcsImlkZW50aWZ5SWQiOjU2LCJzZXF1ZW5jZU51bWJlciI6MTMzfQ==",
            "kw.pv_session": "18",
            "_sp_id.fb3f": "808eea84-5269-4e28-850e-f1d19fe5e3f8.1734941752.6.1737623649.1737618350.78d1259f-04ce-4193-897a-7dd9e62ab9d4",
            "_chartbeat5": "687|8995|%2Frecherche%2F|https%3A%2F%2Fwww.lemonde.fr%2Frecherche%2F%3Fsearch_keywords%3DImpact%2520trafic%26start_at%3D19%2F12%2F1944%26end_at%3D23%2F01%2F2025%26search_sort%3Drelevance_desc%26page%3D2|B-W9_kBnQpZrDA-y3VDFMtCJCwsyGo||c|DFuTQQDRRVOCCikmA3C1ISiNCWGXDT|lemonde.fr|"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//section[@class='js-river-search']//section/a/@href").extract()

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
            # print(item)
            items = Item()
            items.article_url = item
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.lemonde.fr/recherche/"
        params = {
            "search_keywords": f"{current_keyword}",
            "start_at": "19/12/1944",
            "end_at": "23/01/2025",
            "search_sort": "relevance_desc",
            "page": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//p[@class='article__paragraph ']//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items.content)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
