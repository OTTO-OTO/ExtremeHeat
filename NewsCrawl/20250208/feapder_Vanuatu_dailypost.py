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
        MYSQL_DB="TropicalCyclone",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Vanuatu'
    table = 'Vanuatu'
    #英语
    keywords = ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://www.dailypost.vu/search/"
            params = {
                # "tncms_csrf_token": "e768768f3faccae8335c143510286c1cfc1109bec440a5141b0821c6447742d3.186707f90c05c2dce112",
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
            "referer": "https://www.dailypost.vu/search/?tncms_csrf_token=e768768f3faccae8335c143510286c1cfc1109bec440a5141b0821c6447742d3.186707f90c05c2dce112&l=25&sort=relevance&f=html&t=article%2Cvideo%2Cyoutube%2Ccollection&app%5B0%5D=editorial&nsa=eedition&q=heavy+rain&o=25",
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
            # "tncms_csrf_token": "e768768f3faccae8335c143510286c1cfc1109bec440a5141b0821c6447742d3.186707f90c05c2dce112",
            "InstiSession": "eyJpZCI6IjllY2M1MzMyLWIzZmItNDU5Mi05MmMzLTFhYWQ4ZTkxYTE2YyIsInJlZmVycmVyIjoiIiwiY2FtcGFpZ24iOnsic291cmNlIjpudWxsLCJtZWRpdW0iOm51bGwsImNhbXBhaWduIjpudWxsLCJ0ZXJtIjpudWxsLCJjb250ZW50IjpudWxsfX0=",
            "_ga": "GA1.1.2108123451.1738991616",
            "instiPubProvided": "3149c990-1e8c-44a4-a7b2-e9fdb397f1f9",
            "_pubcid": "3adaf5c4-a342-40d7-b3ab-2cf97cdc6c6f",
            "_pubcid_cst": "zix7LPQsHA%3D%3D",
            "_lr_retry_request": "true",
            "_lr_env_src_ats": "false",
            "_pbjs_userid_consent_data": "3524755945110770",
            "plsVisitorGeo": "HK",
            "plsVisitorCity": "",
            "panoramaId_expiry": "1739596396070",
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "panoramaId": "dd6967eb7842afd67768334a0c8d16d53938761497b942a43ecff32adb2f1512",
            "plsVisitorIp": "141.11.77.143",
            "plsGeoObj": "{\"ip\":\"141.11.77.143\",\"country\":\"HK\",\"region\":\"\",\"city\":\"\",\"zip\":\"\",\"location\":\"22.2842,114.1759\"}",
            "cto_bidid": "ivL-M19FWFlEZDFWNTU2ZUpNQ3ZRN2pRMllLb0h6YklDJTJCUENrQnFxMnMxZTMlMkZpS2x3MTBtSnhKVVhRZGc4OExWJTJCNVVPcU1iS0I3Q0xGMnFIJTJGSVF6THRaZTlBJTNEJTNE",
            "ajs_anonymous_id": "cbf21995-e4e1-4fb5-8d34-bc9568adbf9c",
            "cto_dna_bundle": "lhf-IF9PT1JQUkJIcFlMNFJhcloxd0pnNGdSZjV4Y3BGUGg5dk5mZU01djluS0glMkJKUzQ4QmYzM2xMTm9rVk9LbEVQaHNxYUczb3c5VHd6YkVsUWMlMkZ6Ump3NlElM0QlM0Q",
            "_gid": "GA1.1.1916136720.1738991620",
            "cto_bundle": "94C3RV9PT1JQUkJIcFlMNFJhcloxd0pnNGdSY1E2ZU1NSzdOc0h2TUhqeVJ3Yk5KMmlvM3M4dG1YdUtrQWtGc2hZWnlpdW5mbFY2JTJGRjZGRGxheSUyQkpnQlMyZVJzY1l2b3A1aWg3NEdCZCUyRmt5cnNzTWRWWXczemhGcENpd1JLRE1INjNDeHE2TyUyQnBlQWtheE1GJTJGcVAlMkJBSWsyQXclM0QlM0Q",
            "_ga_4T2EB147B8": "GS1.1.1738991616.1.1.1738991769.60.0.0",
            "_ga_XJB70ZFBBZ": "GS1.1.1738991616.1.1.1738991787.42.0.0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        # print(type(response.text))
        # print(type(response.json))
        # links = json.loads(response.text)
        # print(response.text)
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
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 25
        url = "https://www.dailypost.vu/search/"
        params = {
            # "tncms_csrf_token": "e768768f3faccae8335c143510286c1cfc1109bec440a5141b0821c6447742d3.186707f90c05c2dce112",
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
        items.title = response.xpath("//title//text()").extract_first()
        items.content = "".join(response.xpath("//div[@id='article-body']//p//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
