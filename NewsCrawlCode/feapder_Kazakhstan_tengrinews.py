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


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[6, 8],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB="other_country_site3",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Kazakhstan'
    table = 'Kazakhstan_tengrinews'
    keywords =  [
    "жара", "экстремальные жары", "высокая температура", "экстремальные температуры", "жаровые волны", "увеличение высокой температуры",
    "влияние высокой температуры", "высокая температура", "сильная жара", "ышповение температуры", "жаровые события", "повышение температуры",
    "сильные дожди", "обильные осадки", "ливни", "экстремальные дожди", "засуха", "серьезная засуха", "длительная засуха", "дефицит водных ресурсов",
    "отключение электроэнергии", "отключение электроэнергии из-за жары", "отключение электроэнергии из-за жаровой волны", "отключение электроэнергии из-за высокой температуры",
    "пожары", "пожары из-за высокой температуры", "жаровые пожары", "пожары из-за температуры", "пожары, вызванные высокой температурой",
    "влияние на сельское хозяйство", "жаровые волны в сельском хозяйстве", "повреждение урожая", "тепловой стресс в сельском хозяйстве",
    "гипоксия", "тепловой удар", "жаровой гипоксия", "тепловой удар от высокой температуры", "влияние на транспорт", "транспорт из-за высокой температуры",
    "транспорт из-за жаровой волны", "транспорт из-за температуры", "экологическая катастрофа", "катастрофа от жары", "высокая температура окружающей среды",
    "влияние жары на биоразнообразие", "жаровые волны в экологии", "загрязнение", "загрязнение из-за высокой температуры", "тепловое загрязнение",
    "загрязнение температуры", "белое отбеливание кораллов", "высокая температура коралловых рифов", "отбеливание кораллов из-за температуры"
]
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://tengrinews.kz/search/page/2/"
            params = {
                "field": "all",
                "text": f"{keyword}",
                "sort": "date"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, method='GET',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://tengrinews.kz/search/?text=%D0%B6%D0%B0%D1%80%D0%B0",
            "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Microsoft Edge\";v=\"133\", \"Chromium\";v=\"133\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//div[@class='content_main_item']/a/@href").extract()

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
        url = f"https://tengrinews.kz/search/page/{current_page}/"
        params = {
            "field": "all",
            "text": f"{current_keyword}",
            "sort": "date"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, method='GET',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//meta[@property='og:title']/@content").extract_first()
        items.content = "".join(response.xpath("//div[@class='content_main_text']//p/text()").extract()) if response.xpath("//div[@class='content_main_text']//p/text()").extract() else response.xpath("//div[@class='content_main_text']/text() | //h2[@class='content_main_desc']/text()").extract()
        items.author = ''
        items.pubtime = response.xpath("//meta[@name='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
