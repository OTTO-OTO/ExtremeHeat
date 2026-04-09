# -*- coding: utf-8 -*-
import json
import re
import time
import uuid

import feapder
from feapder import Item


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
    country = 'Sweden'
    table = 'Sweden'
    keywords = ["Tropisk cyklon", "Tropisk depression", "Tropisk storm", "Tyfon", "Orkan", "Cyklon", "Storm", "Kraftig regn", "Översvämning", "Stormflod", "Kustskada", "Skred", "Geologisk katastrof", "Marin katastrof", "Kraftiga vindar", "Tyfonkatastrof", "Mudslide", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://www.expressen.se/list-page-next/search-page/"
            params = {
                "q": f"{keyword}",
                "sort": "relevance",
                "page": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=1, i",
            "referer": "https://www.expressen.se/sok/?q=extrema+&sort=relevance",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "__extblt": "49",
            "didomi_token": "eyJ1c2VyX2lkIjoiMTk0NzM2ZWEtNjk1OC02YWMyLWFlNTYtNWY4ODI5OGQ3YTE5IiwiY3JlYXRlZCI6IjIwMjUtMDEtMTdUMDg6NDM6MzkuMDI5WiIsInVwZGF0ZWQiOiIyMDI1LTAxLTE3VDA4OjQzOjUxLjgwNloiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwiYzprcnV4LWRpZ2l0YWwiLCJjOmJuLXNjb3JlMjQiLCJjOmJvbm5pZXJuZXdzLXJleW5vbGRzIiwiYzpibi1kcGEiLCJjOnN2ZW5za2FzcC16OUxUTHJjMyIsImM6Ym4tYWNhc3QiLCJjOmJuLWRhdGF3cmFwcGVyIiwiYzpibi1mYWNlYm9vayIsImM6Ym4tZ2lwaHkiLCJjOmJuLWdvb2dsZW1hcHMiLCJjOmJuLWlmcmFnYXNhdHQiLCJjOmJuLWluc3RhZ3JhbSIsImM6Ym4tdmltZW8iLCJjOmJuLXd1Zm9vIiwiYzpibi15b3V0dWJlIiwiYzpnb29nbGVhbmFseXRpY3MiLCJjOmJuLXR3aXR0ZXIiLCJjOmtsYXJuYWJhbi1hd0V0ZlBkVyIsImM6Ym4tcGFyc2VseSIsImM6Ym4tcmFkaW9wbGF5IiwiYzpibi1zdmVyaWdlc3JhZGlvIiwiYzpzdG9ybWdlbyIsImM6Ym4tcXVhbGlmaW8iLCJjOmJpbHdlYi1YbWFxbmc4RyIsImM6Ym4tc3RheWxpdmUiLCJjOmdvb2dsZWFkcyIsImM6dmlhcGxheSIsImM6Ym4ta2Vlc2luZyIsImM6Ym4tZGlkb21pIiwiYzpzY291dGdnIiwiYzpibi1hZHNlcnZpY2UiLCJjOmJuLWZhY2Vib29rcGl4ZWxuIiwiYzpibi10d2l0Y2giLCJjOm9ydmVzdG8iLCJjOmJuLXRpa3RvayIsImM6Ym4tcHJlbmx5IiwiYzpibi1UVCIsImM6Ym4tZWNob2JveCIsImM6Ym4ta2FudGFybWVkaWFzd2VkZW4iLCJjOmJvbm5pZXJuZXdzIiwiYzpibi1lbGV2ZW5sYWJzIiwiYzpibi1zcG90aWZ5Il19LCJwdXJwb3NlcyI6eyJlbmFibGVkIjpbImRldmljZV9jaGFyYWN0ZXJpc3RpY3MiLCJmdW5rdGlvbmVsLXQybWVZQ2VxIiwiYW5hbHlzdXR2LUJjQ3E0VkVxIiwibWFya25hZHNmby1HNmhOcWdxbSIsImVtYmVkcyIsImNvb2tpZXNmby1FRERVaXhWZCIsImdlb2xvY2F0aW9uX2RhdGEiXX0sInZlbmRvcnNfbGkiOnsiZW5hYmxlZCI6WyJnb29nbGUiXX0sInZlcnNpb24iOjIsImFjIjoiREhHQU9CRWtDaFFGaHdXbGd0eEJmYURIRUFBQS5ESEdBT0JFa0NoUUZod1dsZ3R4QmZhREhFQUFBIn0=",
            "euconsent-v2": "CQLYCcAQLYCcAAHABBENBYFsAP_gAEPgACQgJpQKwABAAIgAUABgAEAAKgAZAA8ACAAGQANAAfABEgCYAJwAWwAugBiADcAHOAQABBACMAGiAP0AiIBHQCRAE7AKyAXUAxQBnADTgHVAUmAsMBbwC8wGMgMsAamA1cCFwEmQJpAkzAeAAQAAsACoAHgAQAAyABoAEQAJgAXQAxAB-AGaANEAfoBFgCOgEiAMUAfYBMgCkwFsgLzAYIAywBqYDVwIXASZAAAA.f_wACHwAAAAA",
            "bau_rmp_freq": "true",
            "bnacid": "d3ac5072-d1a2-41d5-85a0-f6b2560554b9",
            "bnasid": "3c1b4a6e-b10c-4089-a336-7ea6b9ee351a",
            "_ga": "GA1.2.431166732.1737103432",
            "_gid": "GA1.2.1449228503.1737103432",
            "_parsely_session": "{%22sid%22:1%2C%22surl%22:%22https://www.expressen.se/%22%2C%22sref%22:%22%22%2C%22sts%22:1737103432750%2C%22slts%22:0}",
            "_parsely_visitor": "{%22id%22:%22pid=0881132c-8248-4589-83d7-2cb614dbcd8e%22%2C%22session_count%22:1%2C%22last_session_ts%22:1737103432750}",
            "pbjs_sharedId": "28ee2c4d-6ea3-440d-95a8-b2b06a6e70a8",
            "pbjs_sharedId_cst": "sCxZLP8svQ%3D%3D",
            "__codnp": "",
            "bau_deliverance": ""
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//a[@class='list-page__item__link']/@href").extract()
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
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.expressen.se/list-page-next/search-page/"
        params = {
            "q": f"{current_keyword}",
            "sort": "relevance",
            "page": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword,filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='article__card']//p/text() | //article[@id='content']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
