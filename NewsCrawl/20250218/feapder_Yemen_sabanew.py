# -*- coding: utf-8 -*-
"""

集群运行

"""
import re
from datetime import datetime

import feapder
from feapder import Item
import json
import time
from lxml import etree
from sympy import print_tree


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[6, 10],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB="TropicalCyclone",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Yemen'
    table = 'Yemen'
    #阿拉伯语
    keywords =  ["عاصفة استوائية", "اضطراب استوائي", "عاصفة استوائية", "إعصار", "إعصار", "إعصار", "عاصفة", "أمطار غزيرة", "فيضان", "ارتفاع", "أضرار ساحلية", "انهيار تربي", "كارثة جيولوجية", "كارثة بحرية", "رياح قوية", "كارثة الإعصار", "انهيار طيني", "انهيار تربي"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.sabanew.net/search.php"
            params = {
                "lang": "ar",
                "term": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, method='GET',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "Referer": "https://www.sabanew.net/search.php?lang=ar&term=%D8%A3%D8%AD%D8%AF%D8%A7%D8%AB+%D9%85%D9%88%D8%AC%D8%A7%D8%AA+%D8%A7%D9%84%D8%AD%D8%B1%D8%A7%D8%B1%D8%A9",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
            "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Microsoft Edge\";v=\"133\", \"Chromium\";v=\"133\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//div[@class='col-md-12']/a/@href").extract()

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
            # items.title = item.get("title")
            # items.pubtime = item.get("startAt")
            items.article_url = item
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.sabanew.net/search.php"
        params = {
            "lang": "ar",
            "term": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, method='GET',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h5/text()").extract_first()
        items.content = "".join(
            response.xpath(
                "//div[@class='col-md-9']//div[@class='col-md-12']//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//div[@class='col-md-9']//div[@class='col-md-12']/text()").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
