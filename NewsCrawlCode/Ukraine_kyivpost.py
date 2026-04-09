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
    sql = """ select db_name from keywords where country='Ukraine' and language='英语'"""
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

    country = 'Ukraine'
    table = 'Ukraine'
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
    print("待抓取的关键词列===========>", keywords)
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.kyivpost.com/search"
            params = {
                "q": f"{keyword}",
                "page": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page,
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=1, i",
            "referer": "https://www.milliyet.com.tr/haberleri/sicak",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0"
        }
        request.cookies = {
            "_sksid": "e0445b33a5124641845b5abf87bb4bb5",
            "_skulp": "2025%2F2%2F8",
            "_skou": "direct",
            "_skouu": "https%3A%2F%2Fwww.milliyet.com.tr%2F",
            "_sksl": "%5B%22_sksid%22%2C%22js_skinit_id%22%2C%22_skou%22%2C%22_skouu%22%5D",
            "_skrc": "e0445b33a5124641845b5abf87bb4bb5",
            "_skbid": "f13787a528184c5fb714a1c305a4be83",
            "RamadanCity": "9541",
            "isWebSiteFirstVisit": "true",
            "TAPAD": "%7B%22id%22%3A%2209796185-8c2f-403a-9339-fe45c762f7e2%22%7D",
            "_ga": "GA1.1.2129662721.1738977031",
            "_clck": "1ue3fpr%7C2%7Cft9%7C0%7C1865",
            "_ym_uid": "1738977034914235803",
            "_ym_d": "1738977034",
            "_ym_isad": "2",
            "RCookie": "1",
            "isPopupShow": "true",
            "_ym_uid_cst": "kSylLAssaw%3D%3D",
            "OptanonConsent": "isGpcEnabled=0&datestamp=Sat+Feb+08+2025+09%3A13%3A26+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202302.1.0&isIABGlobal=false&consentId=0faf2d02-5d39-4f2f-ae4e-a4e9d96f3fc5&interactionCount=1&landingPath=https%3A%2F%2Fwww.milliyet.com.tr%2F&groups=C0001%3A1%2CC0002%3A0%2CC0003%3A0%2CC0004%3A0%2CC0005%3A0&hosts=H40%3A1%2CH5%3A0%2CH41%3A0%2CH50%3A0%2CH60%3A0%2CH207%3A0%2CH337%3A0%2CH197%3A0%2CH122%3A0%2CH136%3A0%2CH137%3A0%2CH212%3A0%2CH138%3A0%2CH63%3A0%2CH6%3A0%2CH7%3A0%2CH85%3A0%2CH94%3A0%2CH139%3A0%2CH9%3A0%2CH10%3A0%2CH345%3A0%2CH142%3A0%2CH143%3A0%2CH12%3A0%2CH145%3A0%2CH146%3A0%2CH14%3A0%2CH151%3A0%2CH152%3A0%2CH153%3A0%2CH155%3A0%2CH236%3A0%2CH16%3A0%2CH66%3A0%2CH18%3A0%2CH156%3A0%2CH158%3A0%2CH161%3A0%2CH20%3A0%2CH21%3A0%2CH126%3A0%2CH22%3A0%2CH288%3A0%2CH24%3A0%2CH25%3A0%2CH201%3A0%2CH209%3A0%2CH166%3A0%2CH111%3A0%2CH28%3A0%2CH168%3A0%2CH169%3A0%2CH170%3A0%2CH317%3A0%2CH29%3A0%2CH30%3A0%2CH172%3A0%2CH173%3A0%2CH72%3A0%2CH31%3A0%2CH33%3A0%2CH215%3A0%2CH216%3A0%2CH34%3A0%2CH35%3A0%2CH36%3A0%2CH74%3A0%2CH75%3A0%2CH178%3A0%2CH179%3A0%2CH76%3A0%2CH181%3A0%2CH182%3A0%2CH210%3A0%2CH359%3A0%2CH385%3A0%2CH77%3A0%2CH218%3A0%2CH187%3A0%2CH189%3A0%2CH133%3A0%2CH78%3A0%2CH120%3A0%2CH190%3A0%2CH105%3A0%2CH270%3A0%2CH107%3A0%2CH52%3A0%2CH204%3A0%2CH220%3A0&genVendors=V2%3A1%2C",
            "_tfpvi": "MjkyZDY3YmYtZDFkNy00ZTIwLThlMjAtZDQ3NmI5N2Q0OGQ4IzQtNA%3D%3D",
            "js_skinit_id": "98b496b3009c4fd0a4cac78c74361a64",
            "_ga_G0V60V8KPT": "GS1.1.1738977030.1.1.1738977257.5.0.0",
            "FCNEC": "%5B%5B%22AKsRol96BhkrURLZgGKT0OB4cMiXIQxrdLn7NHBkR2Ko5dG7S0loBNIFdzMzXyGeHw2V6-BNQu3qdSYgKPEmHYVc1OHKj8IVcVwlYfIg4uLzl2JsAv9a8cPPcRP1rHAdurhSRV86gMbu-50FoeYP9pLkpPzh-r_lIw%3D%3D%22%5D%5D",
            "_clsk": "9cithx%7C1738977258024%7C3%7C0%7Ck.clarity.ms%2Fcollect"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        # print(type(response.text))
        # print(type(response.json))
        # links = json.loads(response.text)
        links = response.xpath("//div[@class='title']/a/@href").extract()
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
            # items.title = item.get("metadata").get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.kyivpost.com/search"
        params = {
            "q": f"{current_keyword}",
            "page": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1//text()").extract_first()
        items.content = "".join(
            response.xpath("//section[@id='section_0']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
