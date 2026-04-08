# -*- coding: utf-8 -*-
"""

集群运行

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

    country = 'Bangladesh'
    table = 'Bangladesh_bdnews24'
    keywords = ["environment"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://bdnews24.com/cat-load-more"
            params = {
                "page": "1"
            }
            data = {
                "_token": "sv1fr9Q9kLt14GvUJis8qVCttR34spgEDvYEarhe",
                "slug": "environment",
                "posCatIDs": "448224,447894,447620,447473,447405"
            }
            yield feapder.Request(url, params=params,data=data, callback=self.parse_url, page=page, method='POST',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://bdnews24.com",
            "priority": "u=1, i",
            "referer": "https://bdnews24.com/environment",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0",
            "x-requested-with": "XMLHttpRequest"
        }
        request.cookies = {
            "cf_clearance": "_6k2EcYYymwGY6U7xmvfLhz_I5uYfFpXNGcZ8vHj8kg-1739152071-1.2.1.1-XQI5mEqQyg7yqK.gNzCT3Spl4PtzyELoNDK1mUhZ65XI_HMuegMNdXjFS._7El3N2AyQ4NUXr42cV7wBSciekGpt7H8jZmciR9DtK2gtyONRFaGfiIXjyrk6QopeRIcEU9Lpr5Y8lF032Zq9sP8brGgoo4Ab9nXzMsJIZrVq0UDrc4le.UM24a9EYncXG9ldNznkJODbQ5_6ug0Um1GWjcX9akxXxXExoMaept9tEhzWBdLhEDImyrZZQe7rhDsSKKGyO.uq6.eDVRh_CcNKL2_S8ht8TBhXKIU8gb9viX4",
            "fpestid": "rGQ1EFinaam9TuMpfDbDk6k2RBp4gXFZf5FcnlYit1wsRXqfUoIe_aVXYCaEujkUfk-DPQ",
            "_ga": "GA1.1.1798533788.1739152095",
            "_pubcid": "27520e5d-46d1-4b49-8a66-cd98a1d5e204",
            "_pubcid_cst": "zix7LPQsHA%3D%3D",
            "__jscuActive": "true",
            "lses": "1.SzOUQgpBlQcmdKPg4WU434xbuKBivkfD",
            "__gsas": "ID=f470f0301dcb1f53:T=1739152331:RT=1739152331:S=ALNI_MaMqpRLGJRE1I83y-rUByTkqCgtrA",
            "cto_bidid": "R8wZA19EdTM4dmhPdXYlMkZJUmZGUjAyUkM3MVdGYlRjQ3paMmdSZ2VHNE5XNTI2blhNSGIzRGlIdEdFS2pqR2V0ZlpGNmRUcW1VQnVDejNlN0lSRE8zMnZiMG9vYzdUcTkzRkxKVzdHQ0t3WVFJOXhFJTNE",
            "cto_dna_bundle": "J3ejCl9PT1JQUkJIcFlMNFJhcloxd0pnNGdSZjV4Y3BGUGg5dk5mZU01djluS0glMkJKUzQ4QmYzM2xMTm9rVk9LbEVQaHNtQzlmRHpXMHdrcWVHaEpPZ2FCWXVnJTNEJTNE",
            "_ga_9G1RVG4MKV": "GS1.1.1739152094.1.1.1739152539.39.0.0",
            "cto_bundle": "4bz_hV9PT1JQUkJIcFlMNFJhcloxd0pnNGdmazhiRGF3eUhWZnZ3T1BsZ2Q5NXZFZyUyRmtER3AyWTRjOW9YYXVsSmlNSFlMUHZkUmdvQW5EZU90aHgza0FyT241cmo4Q0xDTSUyQjY5OWdWJTJGOTl3U28lMkY4SXlISFpvTjhJSVBiMSUyRjhSU3JNeWdJd096ejBlbzloQmhUbUglMkYlMkJ6UFRzdyUzRCUzRA",
            "XSRF-TOKEN": "eyJpdiI6Ik9UaXpTN2F5Z3NUOFR4WitpcDY4U2c9PSIsInZhbHVlIjoidm5jc05aSjd5WFl3cWhMc2o2L3VUOGE2YVVlU001ZFRncHc4dnNzMWJkRkJjL21vQnBaaTRkUW1qT0ptTTZ5NXZjL0RwOEFGRXU0Q0R6dHdwa1ZLNURsTFFSejhoZ3lzbVlNQU8xWFZ6ck9odkRvTWpWeTNrajlRdmtmdW9MdkEiLCJtYWMiOiJhNTgyMDJhNmRjYjBhMzhiZGE4MjQ4OGJiZDdlYTFlNTQzYmE1MGFlOGVkOTlmNWVkYjVhNGRkNjU1OTFiNTM3IiwidGFnIjoiIn0%3D",
            "bdnews24_session": "eyJpdiI6IjVyZWJhajVpclBzK3I2M1VFNnVJVEE9PSIsInZhbHVlIjoiaW5vS3pzdHZqcVJLZUxxQlRZQVdHRUgrUXNvdHVVZUdZZ1ZCN0c1S2o0WjFFclZueWUwei9BTnY5d053SmtNdVhja2M2VHdWUGJiVVJoNTBKYWc2Sk1jL2haOWZEdCt0N0NOVUNqOW5jM0pPem9ZMjQ2R1crbUpLVWlRYmpvR3ciLCJtYWMiOiIwN2Y3MzkzODU0NDRkNzgwN2E5YmZmYWI1YzI4ZWMyNDkxMmVmNDI5M2VkZTk2NTk0ZTJhYmExNWNlZTgwN2I5IiwidGFnIjoiIn0%3D"
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
        data = response.json['html']
        links = etree.HTML(data).xpath("//div[@class='rm-container align-items-stretch']/a/@href")
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
        url = "https://bdnews24.com/cat-load-more"
        params = {
            "page": f"{current_page}"
        }
        data = {
            "_token": "sv1fr9Q9kLt14GvUJis8qVCttR34spgEDvYEarhe",
            "slug": f"{current_keyword}",
            "posCatIDs": "448224,447894,447620,447473,447405"
        }
        yield feapder.Request(url, params=params,data=data, callback=self.parse_url, page=current_page, method='POST',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@id='contentDetails']//p/text()").extract())
        items.author = ''
        items.pubtime = ''
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
