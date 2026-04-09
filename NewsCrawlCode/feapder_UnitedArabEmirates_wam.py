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

    country = 'United Arab Emirates'
    table = 'United_Arab_Emirates'
    #英语
    keywords = ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://www.wam.ae/api/app/articles/search"
            data = {
                "skipCount": 0,
                "maxResultCount": 20,
                "requiredMediaTypeq": None,
                "q": f"{keyword}"
            }
            data = json.dumps(data, separators=(',', ':'))
            yield feapder.Request(url, data=data, callback=self.parse_url, page=page, method='POST',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Origin": "https://www.wam.ae",
            "Referer": "https://www.wam.ae/en/search",
            # "RequestVerificationToken": "CfDJ8HH9RKSTj4RHoorYKVjEVgLDghJ2cIIc4t88-Nfh4k73L5dpyem5yWW3Ps3Mi_CHm6PaQPbnsDcJPTLWogUdn_Xcs-4_Ncxmkli2U-4KpEl47Uub7RRarEaTf-hyYPdZne0uvjX3vZr6GOxwWrSdD-c",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0",
            "X-Requested-With": "XMLHttpRequest",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        request.cookies = {
            "BIGipServer~DMZ_WAM~prod-web.wam.ae_vs_pool": "rd3o00000000000000000000ffffc0a80382o443",
            ".AspNetCore.Antiforgery.WhyV0PhV1ak": "CfDJ8HH9RKSTj4RHoorYKVjEVgK4iYA6jhlceYhkuPg1ZQCt3ufYKjnVXHtACPUd5Kzaqxj68nBTaRQeLiA_eQ0MUplPoCyrdlHN_-t8FWRM4F7xbwDaWxcUeJJ7JH_84DhAeJamsjdu13QGWVkko-EDwLc",
            "_ga": "GA1.1.290386890.1738981108",
            ".AspNetCore.Culture": "c%3Den%7Cuic%3Den",
            # "XSRF-TOKEN": "CfDJ8HH9RKSTj4RHoorYKVjEVgLDghJ2cIIc4t88-Nfh4k73L5dpyem5yWW3Ps3Mi_CHm6PaQPbnsDcJPTLWogUdn_Xcs-4_Ncxmkli2U-4KpEl47Uub7RRarEaTf-hyYPdZne0uvjX3vZr6GOxwWrSdD-c",
            "TS01ae0ff0": "011c2fe8e9c83719e3a539b1ef265acd7aff526991ae57f46f65cddb04d5206f16fedaa598adf7baabc2616c9401d4dec1c5308fb93d7b7c47943956840beb18e9518904c76d4cf618dae7a8c0336bf4fa3fb9fa2fe0a0b58e7f93066500c64decbed03af6",
            "_ga_LNB1QE7TZ5": "GS1.1.1738981108.1.1.1738981141.0.0.0",
            "TS16dc9098027": "08d15a8d79ab2000f0ede07f48fd5eb07950e13b4bc1492c2055983574258c1809fdba147c60f3bc084a0c9cd4113000441ed172d5a833554a8e0d4dda84264ad201d17a1ee1a32189847c77e8c4c61e5419f0087ff63e361d4d94a32c6ec00f"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        # print(type(response.text))
        # print(type(response.json))
        # links = json.loads(response.text)
        links = response.json['items']
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
            items.article_url = "https://www.wam.ae/api/app/articles/GetArticleBySlug?slug=" + item.get("urlSlug")
            items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 20
        url = "https://www.wam.ae/api/app/articles/search"
        data = {
            "skipCount": current_page,
            "maxResultCount": 20,
            "requiredMediaTypeq": None,
            "q": f"{current_keyword}"
        }
        data = json.dumps(data, separators=(',', ':'))
        yield feapder.Request(url, data=data, callback=self.parse_url, page=current_page, method='POST',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        # items.title = response.xpath("//h1//text()").extract_first()
        items.content = "".join(etree.HTML(response.json['body']).xpath("//p//text()"))
        items.author = ''
        items.pubtime = response.json['articleDate']
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
