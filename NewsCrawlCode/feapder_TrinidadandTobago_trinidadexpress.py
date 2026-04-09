# -*- coding: utf-8 -*-
"""

本地运行

"""
import re

import feapder
from feapder import Item
import json
from lxml import etree


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[8, 10],
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

    country = 'Trinidad and Tobago'
    table = 'Trinidad_and_Tobago'
    #英语
    keywords =  ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://trinidadexpress.com/search/"
            params = {
                # "tncms_csrf_token": "d0e33494e4f8137c5551a5ccd9045a8ddecb89ba11385199c3809e52bef2c88e.aec3e51b8e9e2a2bd50c",
                "l": "25",
                "sort": "relevance",
                "f": "html",
                "t": "article,video,youtube,collection",
                "app%5B0%5D": "editorial",
                "nsa": "eedition",
                "q": f"{keyword}",
                "o": "0"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page,
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://trinidadexpress.com/search/?tncms_csrf_token=d0e33494e4f8137c5551a5ccd9045a8ddecb89ba11385199c3809e52bef2c88e.aec3e51b8e9e2a2bd50c&l=25&sort=relevance&f=html&t=article%2Cvideo%2Cyoutube%2Ccollection&app%5B0%5D=editorial&nsa=eedition&q=heatwave&o=25",
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
            "_gcl_au": "1.1.695489546.1738975465",
            # "tncms_csrf_token": "d0e33494e4f8137c5551a5ccd9045a8ddecb89ba11385199c3809e52bef2c88e.aec3e51b8e9e2a2bd50c",
            "ta_MACHINE_ID": "de0be057a29b60bfbae586b2330fd6ac",
            "logglytrackingsession": "ba9d6406-1fdf-4a83-a546-36121f0b04b4",
            "_cb": "BMWQKeC1c-0YDiWkSj",
            "ta_cookiesTest": "1",
            "ta_vl": "1",
            "ta_ss": "d6e0580d87803b36c672518708638dfd",
            "_ga": "GA1.1.333602354.1738975472",
            "ajs_anonymous_id": "cb6d2ccf-2975-42f0-9668-ceeddfad1c70",
            "__gads": "ID=4ee190b1689db4b1:T=1738975454:RT=1738975454:S=ALNI_MZViGZS4tLaISNn8m8K9oWxODUTog",
            "__gpi": "UID=0000102d178bc003:T=1738975454:RT=1738975454:S=ALNI_MYTbj956MRtb8aA4y7nh1mKXIR8Ng",
            "__eoi": "ID=ff318cf97ad7e545:T=1738975454:RT=1738975454:S=AA-Afjbcxu-im2UC1ZSqEm2nsDrX",
            "__gsas": "ID=5f410d6e700e254d:T=1738975467:RT=1738975467:S=ALNI_Mabs-waLTehrwSSUG5j2sZAnUZVyw",
            "_cb_svref": "external",
            "_ga_4T2EB147B8": "GS1.1.1738975472.1.1.1738975536.60.0.0",
            "_ga_4SSDFHN6CH": "GS1.1.1738975473.1.1.1738975537.59.0.0",
            "refc": "%3Fref%3Dtrinidadexpress.com%2Fsearch%2F%26refq%3Dheatwave",
            "_chartbeat2": ".1738975469733.1738975537522.1.CPspunBOwB2wBWWnGjCvRFJdBpgBMY.3",
            "ta_interrupted": "0",
            "_awl": "2.1738975517.5-d5c51caa1c932a4430d2962672365d3d-6763652d617369612d6561737431-0",
            "FCNEC": "%5B%5B%22AKsRol84u10F23c5I-Q0HVN8Ocs-5TGJ2u0WjRjhOKkMGox0bEVNyaH-znEqgrlDLGINnObAZtkpFFK88XWR44BRuHWiMEwYYis8KfPfXNtB5yfWaGTpIX8fMK4qfHKcegqI-va0wBxiaisjO1lGJrelraeNHNkaUg%3D%3D%22%5D%5D",
            "_chartbeat5": "1120|5832|%2Fsearch%2F|https%3A%2F%2Ftrinidadexpress.com%2Fsearch%2F%3Ftncms_csrf_token%3Dd0e33494e4f8137c5551a5ccd9045a8ddecb89ba11385199c3809e52bef2c88e.aec3e51b8e9e2a2bd50c%26l%3D25%26sort%3Drelevance%26f%3Dhtml%26t%3Darticle%252Cvideo%252Cyoutube%252Ccollection%26app%255B0%255D%3Deditorial%26nsa%3Deedition%26q%3Dheatwave%26o%3D50|Drzbl4CFARZwXO6_eBrmYk9X_3ls||c|2EK4HDXrXDeD7DdC8D03Pq6DE7Fpk|trinidadexpress.com|"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        # print(type(response.text))
        # print(type(response.json))
        # links = json.loads(response.text)
        links = response.xpath("//h3/a/@href").extract()
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

        current_page = request.page + 25
        url = "https://trinidadexpress.com/search/"
        params = {
            # "tncms_csrf_token": "d0e33494e4f8137c5551a5ccd9045a8ddecb89ba11385199c3809e52bef2c88e.aec3e51b8e9e2a2bd50c",
            "l": "25",
            "sort": "relevance",
            "f": "html",
            "t": "article,video,youtube,collection",
            "app%5B0%5D": "editorial",
            "nsa": "eedition",
            "q": f"{current_keyword}",
            "o": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1//text()").extract_first()
        items.content = "".join(response.xpath("//div[@id='article-body']//p//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
