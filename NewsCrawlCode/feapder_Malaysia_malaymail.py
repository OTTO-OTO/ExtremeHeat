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
        MYSQL_DB="other_country_site2",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Malaysia'
    table = 'Malaysia'
    #英语
    keywords = ["heatwave","Extreme heatwave", "High temperature", "Extreme temperature", "Heatwave event", "Increase in high temperature", "High temperature impact", "Strong heat", "Temperature rise", "Heat event", "Temperature increase", "Heavy rainfall", "Heavy precipitation", "Torrential rain", "Extreme rainfall", "Drought", "Severe drought", "Long-term drought", "Water shortage", "Power outage", "High temperature power outage", "Heatwave power outage", "Power outage caused by high temperature", "Fire", "High temperature fire", "Heat fire", "Temperature fire", "Heat-induced fire", "Agricultural impact", "Heatwave agriculture", "Crop damage", "Agricultural heat stress", "Hypoxia", "Heatstroke", "Heat-induced heatstroke", "High temperature hypoxia", "High temperature heatstroke", "Traffic impact", "High temperature traffic", "Heatwave traffic", "Temperature traffic", "Ecological disaster", "Heat disaster", "High temperature environment", "Impact of heat on biodiversity", "Heatwave ecology", "Pollution", "High temperature pollution", "Heat pollution", "Temperature pollution", "Coral bleaching", "High temperature coral reefs", "Temperature coral bleaching" ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.malaymail.com/search"
            params = {
                "query": f"{keyword}",
                "pgno": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.malaymail.com/search?query=heavy+rain",
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
            "device": "web",
            "_pbjs_userid_consent_data": "3524755945110770",
            "am_FPID": "088c9a47-a9e1-438b-bcb6-f6c5d572fc2b",
            "am_FPID_JS": "088c9a47-a9e1-438b-bcb6-f6c5d572fc2b",
            "_gcl_au": "1.1.595305459.1737620525",
            "cf_clearance": "yisd6PIJatY8059ITD1awW2yQRaCev6qqAjkA8ZI7yg-1737620510-1.2.1.1-g_it0bX5N2VtyhmzY1HVmWJBFUz8er6gkoiBISwISazWr01pcjFhFj.oABI1O8EzcKSjvJaekHcauXZSQPvtLPBPmjopePfwbHvWMnIgGdXbzVcADjpeWiZCjvjCWJRacB6.ijAn4ttM18o6Pd2J1IiU2I5YmBLrNgagK6ji2qNgFZfIvrNcK.G59ejHb6ua.xSdL7s0qF54ws_rDgBjPbo9UNelVRpFqvVonMwiNSev1zMHGOJ.8p_5ebQyhnWY7GLFKZe28UMqsYnlzo.Cy1ainlA63VwE4xxzspZCZKE",
            "_gid": "GA1.2.376950840.1737620525",
            "_gat_UA-117264847-1": "1",
            "_sharedID": "a7525555-e817-4d07-8574-cfde613792cf",
            "_sharedID_cst": "zix7LPQsHA%3D%3D",
            "_fbp": "fb.1.1737620525730.56663977167252575",
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "panoramaId_expiry": "1738225315301",
            "panoramaId": "dd6967eb7842afd67768334a0c8d16d53938761497b942a43ecff32adb2f1512",
            "panoramaIdType": "panoIndiv",
            "_ga": "GA1.2.2078998910.1737620525",
            "cto_bundle": "79HI3l9PT1JQUkJIcFlMNFJhcloxd0pnNGdXRDdCZDNzdm9mJTJGa3dkemYzOHVKeVg4eHFrRXJ2JTJGZEsxejclMkZFZm9YdiUyQkZJJTJCZ2hZayUyQlRrYk5CaGtYSTh2WnJ0ZHQ2RE45RVk3c1ZodWlFTmc4JTJCRGJEakhRaWdIMkwwS0d6bUFMWm1qWTVWb2pyQW82b1hQZnkwQlVEV0FVNjMxUSUzRCUzRA",
            "cto_bidid": "xxcAnV9hOGlSNVdqclY3Q3pxWVA0czAlMkJtSUVvVWU2cHZMeVlydHptWnVWMktoZ3U4UUZlaUV6T0JXQmJXNWV6NWFFdXFGb2xzN1FFem5oM0phOHNQOWRwTXNYJTJGaCUyRnklMkZqRWFCOTIlMkI0c29mbEhPWW8lM0Q",
            "cto_dna_bundle": "LtSsI19PT1JQUkJIcFlMNFJhcloxd0pnNGdSZjV4Y3BGUGg5dk5mZU01djluS0glMkJKUzQ4QmYzM2xMTm9rVk9LbEVQaHNPVjVnWUpmVG1vb25HTDlHNWklMkJDQUElM0QlM0Q",
            "_tfpvi": "Zjc5ODIzYjYtZDJjZS00NzAzLTk1MGQtYTFmMzEzNzZiNmM0Iy01MA%3D%3D",
            "FCNEC": "%5B%5B%22AKsRol_pg6HLQtNjXu0Hci1AMftaTny3xnovc-2KPjMjWWnTWCE_WPtKVlJkVCDtcK8RotnON-UQkpmCrxzZLaLjHagNJ9gK8YXl0VXmnqUCVvhgXt9AGMv7AAsezfKYVRjp8Tfcfc0_5tgzhd_p1mCMqg3lcAOtzQ%3D%3D%22%5D%5D",
            "_ga_M7E3P87KRC": "GS1.1.1737620525.1.1.1737620563.22.0.1928406626",
            "_ga_CNCBNGX7XD": "GS1.1.1737620525.1.1.1737620570.0.0.0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//div[@class='row']//h2/a/@href").extract()

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
        url = "https://www.malaymail.com/search"
        params = {
            "query": f"{current_keyword}",
            "pgno": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='article-body']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
