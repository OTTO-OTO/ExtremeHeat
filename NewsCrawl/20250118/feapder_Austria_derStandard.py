# -*- coding: utf-8 -*-
# 173
import re

import feapder
from feapder import Item
import json
from lxml import etree


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[6, 10],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB="other_country_site2",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Austria'
    table = 'Austria_derStandard'
    keywords = [
    "Hitze", "Extreme Hitzewelle", "Hochtemperatur", "Extremes Temperatur", "Hitzewellenereignis",
    "Temperaturerhöhung", "Hochtemperaturauswirkungen", "Hochtemperatur", "Starker Hitze",
    "Temperaturanstieg", "Hitzeeereignis", "Temperaturerhöhung", "Starke Regenfälle",
    "Starker Regen", "Sturzregen", "Extremer Regen", "Trockenheit", "Schwere Trockenheit",
    "Langfristige Trockenheit", "Wasserknappheit", "Stromausfall", "Hochtemperaturausfall",
    "Hitzewellenausfall", "Ausfall durch hohe Temperaturen", "Brände", "Hochtemperaturbrände",
    "Hitzebrände", "Temperaturbrände", "Brände durch hohe Temperaturen", "Landwirtschaftliche Auswirkungen",
    "Hitzewellenlandwirtschaft", "Ernteerschöpfung", "Landwirtschaftliche Hitzestress", "Sauerstoffmangel",
    "Hitzekrankheit", "Hitzekollaps", "Hochtemperatur-Sauerstoffmangel", "Hochtemperaturkrankheit",
    "Verkehrsauswirkungen", "Hochtemperaturverkehr", "Hitzewellenverkehr", "Temperaturverkehr",
    "Ökologische Katastrophe", "Hitzekatastrophe", "Hochtemperaturumgebung", "Auswirkungen der Hitze auf die biologische Vielfalt",
    "Ökologie der Hitzewelle", "Verschmutzung", "Hochtemperaturverschmutzung", "Hitzeverschmutzung",
    "Temperaturverschmutzung", "Korallenbleaching", "Hochtemperaturkorallen", "Temperaturbleaching der Korallen"
]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.derstandard.at/search"
            params = {
                "query": f"{keyword}",
                "fd": "1997-01-01",
                "s": "score",
                "p": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.derstandard.at/search?query=Hitze",
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
            "_sp_v1_ss": "1:H4sIAAAAAAAAAItWqo5RKimOUbLKK83J0YlRSkVil4AlqmtrlXRGlY0qG8zKopEZeSCGQW0sLn04JZRiAXs_nhQQAwAA",
            "_sp_v1_p": "925",
            "_sp_v1_data": "844376",
            "_sp_su": "false",
            "tcfs": "1",
            "DSGVO_ZUSAGE_V1": "true",
            "consentUUID": "c5d2f867-cb79-4957-8025-d1f4a93cdff4_40",
            "consentDate": "2025-01-18T02:52:58.602Z",
            "MGUID": "GUID=3f1ec140-a90b-482c-9126-b4b8f0b77eef&Timestamp=2025-01-18T02:53:18&DetectedVersion=&Version=&BIV=2&Hash=DAEAED7EF235D5AF77FE42325F7E0238",
            "_pcid": "%7B%22browserId%22%3A%22m61lg9jn8vijtg9y%22%7D",
            "_ga": "GA1.1.156298467.1737168801",
            "__pat": "3600000",
            "MGUIDBAK": "GUID=3f1ec140-a90b-482c-9126-b4b8f0b77eef&Timestamp=2025-01-18T02:53:18&DetectedVersion=&Version=&BIV=2&Hash=DAEAED7EF235D5AF77FE42325F7E0238",
            "FPID": "FPID2.2.qIYk40gv2WUB6ljmo9nqEPQWpf1IKHXAJKpjuWDdckM%3D.1737168801",
            "_sotmsid": "0:m61lgbn5:QNIjsIpwgb_W_G5PBm3_BOuB2hPjBoHX",
            "_sotmpid": "0:m61lgbn5:i8mJxwczaDqJ6LfmxZ3w_yv0VkahT2AE",
            "_hjSessionUser_1013083": "eyJpZCI6ImFjMDI2MTBiLTg5ZTctNTlhYi1hZDU1LTVlNjhjYjYwYzk4YSIsImNyZWF0ZWQiOjE3MzcxNjg4MDQxMDUsImV4aXN0aW5nIjp0cnVlfQ==",
            "_hjSession_1013083": "eyJpZCI6IjY1MDkxY2RiLTQzNzMtNDExMC04NTZjLWIwZGVhNTNmOGRmMyIsImMiOjE3MzcxNjg4MDQxMDYsInMiOjEsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MX0=",
            "_hjHasCachedUserAttributes": "true",
            "_autuserid2": "7461083136394005259",
            "cX_P": "m61lg9jn8vijtg9y",
            "cX_G": "cx%3A1sher8asw173e14a8tv7vve12l%3A213p2lkxgcd1w",
            "__adblocker": "false",
            "FPLC": "cvwuyBAbzTXQB3HmsEbSxriS2WFxNAgy7ZCEVcJB3b%2BkJPu%2BoBlNjnNGNdwDPluuDhWSn5p8U%2F0xXFezGvnGDCDrK%2BsRDxsij4Dbzr1BhTCaO8YXqukaXwQttSa6tA%3D%3D",
            "__pvi": "eyJpZCI6InYtbTYxbGc5anpkNjg4Z2d0cSIsImRvbWFpbiI6Ii5kZXJzdGFuZGFyZC5hdCIsInRpbWUiOjE3MzcxNjk2ODQxMzV9",
            "__pnahc": "0",
            "ioam2018": "0004471d9a4a5b7a1678b1793%3A1764730402734%3A1737168802734%3A.derstandard.at%3A2%3Aat_w_atderstand%3AService%2FSuchmaschinen%2FallgemeineSuche%3Anoevent%3A1737169684188%3A81jh6x",
            "_pctx": "%7Bu%7DN4IgrgzgpgThIC5QGYBmBGKBjdAWADALQCGAnPgEaG4AcATFoaenQGyEW4U2qUDsfKFFTAsAawCWAX0SgADjGESAHohDiJIADQgALgE85UNQGEAGiClSdkWAGVdxXZDXEAdgHs32kBAm6oAEkAEzU6XD4aXFxkXFJw3HRWPBZkSyA",
            "__tbc": "%7Bkpex%7DKwim2AGmhpPgXtJFcpYQrmqay87HhLGBzOY-immWLb7tsUebBnF0DgsHfuvrxOjQ",
            "xbc": "%7Bkpex%7DgXUtUnyVUab0Vzut1VKQrdMCrw2mJ3_PWw_R3R-Un_N0aSs8WlMygbcODC-9kI8G1QOolB7L0nHowYLkdi4C3TvxDr9fN3evoqciVmOPeJE",
            "FPGSID": "1.1737168787.1737169724.G-TQ3BNDRZZ9.Be7dmeq0yRjsYXIdtpKPFg",
            "_ga_TQ3BNDRZZ9": "GS1.1.1737168801.1.1.1737169741.0.0.1706656765"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")

        links = response.xpath("//article/div/a/@href").extract()

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
            items.table_name = self.table
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.derstandard.at/search"
        params = {
            "query": f"{current_keyword}",
            "fd": "1997-01-01",
            "s": "score",
            "p": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='article-body']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
