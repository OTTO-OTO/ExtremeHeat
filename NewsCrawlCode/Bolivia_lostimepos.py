import json

import feapder
from feapder import Item
import re

from feapder.db.mysqldb import MysqlDB

class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.101.200", port=3307, db="spider_keywords", user_name="czm", user_pass="root"
    )
    sql = """ select db_name from keywords where country='Bolivia' and language='西班牙语'"""
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
    country = 'Bolivia'
    table = 'Bolivia'
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
    previous_links = None

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://www.lostiempos.com/hemeroteca"
            params = {
                "contenido": f"{keyword}",
                "sort_by": "field_noticia_fecha",
                "page": f"0"
            }
            yield feapder.Request(url, params=params, method="GET", callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Referer": "https://www.lostiempos.com/hemeroteca?contenido=Calor&sort_by=field_noticia_fecha&page=100",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        request.cookies = {
            "_ym_uid": "1734509990868474767",
            "_ym_uid_cst": "zix7LPQsHA%3D%3D",
            "_fbp": "fb.1.1734509990486.17123014557531815",
            "fpestid": "WoHrdrcemxok0E_ILR0jgtNRO7TnqEsm7ihZgG-i9Er1wZazGeLUAhjeQaYMP9jVKbuFpQ",
            "nvggid": "null",
            "nvggid_cst": "zix7LPQsHA%3D%3D",
            "GN_USER_ID_KEY": "d53ef624-33f4-4b45-8d92-c0a307fcf359",
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "has_js": "1",
            "usrlt": "jjmg1735179348044",
            "_gid": "GA1.2.167742379.1735179350",
            "GN_SESSION_ID_KEY": "6a7a9850-0697-4c0f-b4c2-390eee30ddf7",
            "nvg60118": "15a43ef4b1e6aea12de384ca5510|0_362",
            "MgidStorage": "%7B%220%22%3A%7B%22svspr%22%3A%22%22%2C%22svsds%22%3A2%7D%2C%22C1544841%22%3A%7B%22page%22%3A1%2C%22time%22%3A%221735179354856%22%7D%2C%22C1544837%22%3A%7B%22page%22%3A1%2C%22time%22%3A%221735179354875%22%7D%7D",
            "panoramaId_expiry": "1735784152248",
            "panoramaId": "3d6e8b78d1ec91ae6e29e218acf4185ca02c60046cf65798b2f03d4a5c35863c",
            "panoramaIdType": "panoDevice",
            "nvg46575": "159bb15788bb125187158bb99910|0_362",
            "_gat_gtag_UA_8863683_1": "1",
            "viewslts": "0",
            "_ga": "GA1.2.1907168393.1734509989",
            "_ga_TKL4CPVX70": "GS1.1.1735179348.3.1.1735179565.19.0.0",
            "cto_bundle": "c5Ji5F9PT1JQUkJIcFlMNFJhcloxd0pnNGdXU1JSRmk3dTViUlBXMDU3azI4RWlIeVh4c2JkdlozQXN4MUYzT2tvMERTNTFVMXdtTXVDTXJQc1Y5OTN6cU1QeDdjRzdSTWYyWDBGeGVvdUtZZDVnUWs3UTlZSzVHQkRCWCUyRlhHVDNmZlElMkJyZ3hiQVh2JTJCbDlVcGZ3RFBTNDZxaVElM0QlM0Q",
            "cto_bidid": "rUAv_F9qVHVYQmpFUXAlMkZ3TCUyRk5taXB3VGExcUI0TFRmeWh2c290aTZhQjRWZXN6Y2k1Y05jTERuanRsNkFUVkFnOHclMkZpT3VxaXVSZFJtZVJYJTJCQjFrZ3A3a1Z0am5pOFFOVmliU1FpYSUyRmZoUSUyRklxQll2NXk0dFBUMnFiYTlicUhLTUJFMA",
            "cto_dna_bundle": "lsQZQ19PT1JQUkJIcFlMNFJhcloxd0pnNGdSZjV4Y3BGUGg5dk5mZU01djluS0glMkJKUzQ4QmYzM2xMTm9rVk9LbEVQaHMlMkJleVc5SXJQWE9UTGdPWGtkRkoxeHclM0QlM0Q"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")

        links = response.xpath(
            "//div[@class='view-content']//div[@class='views-field views-field-title']/span/a/@href").extract()[:10]
        current_page = request.page

        current_links = links
        if self.previous_links is not None and current_links == self.previous_links:
            print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理

        self.previous_links = current_links  # 更新上一页的链接列表
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for url in links:
            print(url)
            items = Item()
            items.article_url = url
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.lostiempos.com/hemeroteca"
        params = {
            "contenido": f"{current_keyword}",
            "sort_by": "field_noticia_fecha",
            "page": f"{current_page}",
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='body']//p//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//div[@class='date-publish']/text()").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
