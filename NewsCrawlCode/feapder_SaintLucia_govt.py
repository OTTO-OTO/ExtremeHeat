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
        MYSQL_DB="other_country_site2",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Saint Lucia'
    table = 'SaintLucia_govt'
    keywords = ["heatwave","Extreme heatwave", "High temperature", "Extreme temperature", "Heatwave event", "Increase in high temperature", "High temperature impact", "Strong heat", "Temperature rise", "Heat event", "Temperature increase", "Heavy rainfall", "Heavy precipitation", "Torrential rain", "Extreme rainfall", "Drought", "Severe drought", "Long-term drought", "Water shortage", "Power outage", "High temperature power outage", "Heatwave power outage", "Power outage caused by high temperature", "Fire", "High temperature fire", "Heat fire", "Temperature fire", "Heat-induced fire", "Agricultural impact", "Heatwave agriculture", "Crop damage", "Agricultural heat stress", "Hypoxia", "Heatstroke", "Heat-induced heatstroke", "High temperature hypoxia", "High temperature heatstroke", "Traffic impact", "High temperature traffic", "Heatwave traffic", "Temperature traffic", "Ecological disaster", "Heat disaster", "High temperature environment", "Impact of heat on biodiversity", "Heatwave ecology", "Pollution", "High temperature pollution", "Heat pollution", "Temperature pollution", "Coral bleaching", "High temperature coral reefs", "Temperature coral bleaching" ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.govt.lc/api/services.asmx/GetSearchSummaries"
            data = {
                "query": {
                    "ResourceTypeNames": None,
                    "FilterType": 6,
                    "FilterValue": f"{keyword}",
                    "StartDate": "0001-01-01T05:00:00.0000000Z",
                    "EndDate": "0001-01-01T05:00:00.0000000Z",
                    "StatusFilter": 2,
                    "ParentResourceId": None,
                    "ParentResourceMatchType": 0,
                    "FriendlyDescription": None,
                    "SortType": 0,
                    "IncrementalSearch": None,
                    "Route": {
                        "Subject": None,
                        "Preposition": None,
                        "Object": None
                    }
                },
                "cursor": {
                    "StartRowIndex": 1,
                    "PageSize": 15
                }
            }
            data = json.dumps(data, separators=(',', ':'))
            yield feapder.Request(url, data=data, callback=self.parse_url, page=page, keyword=keyword, method='POST',
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "content-type": "application/json; charset=UTF-8",
            "origin": "https://www.govt.lc",
            "priority": "u=1, i",
            "referer": "https://www.govt.lc/search/heavy%20rain",
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
            "__AntiXsrfToken": "95c267e492d64a00853dcf5d59c81c60",
            "_ga": "GA1.2.1633807767.1738890700",
            "_gid": "GA1.2.504353615.1738890700",
            "_gat": "1",
            "_ga_VYW2SXBP05": "GS1.2.1738890700.1.1.1738890795.0.0.0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        # print(type(response.text))
        # print(type(response.json))
        # links = json.loads(response.text)
        links = response.json.get('d').get("Result").get('Items')
        print(links)
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
            items.article_url = "https://www.govt.lc" + item.get("Url")
            items.title = item.get("Title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 15
        url = "https://www.govt.lc/api/services.asmx/GetSearchSummaries"
        data = {
            "query": {
                "ResourceTypeNames": None,
                "FilterType": 6,
                "FilterValue": f"{current_keyword}",
                "StartDate": "0001-01-01T05:00:00.0000000Z",
                "EndDate": "0001-01-01T05:00:00.0000000Z",
                "StatusFilter": 2,
                "ParentResourceId": None,
                "ParentResourceMatchType": 0,
                "FriendlyDescription": None,
                "SortType": 0,
                "IncrementalSearch": None,
                "Route": {
                    "Subject": None,
                    "Preposition": None,
                    "Object": None
                }
            },
            "cursor": {
                "StartRowIndex": {current_page},
                "PageSize": 15
            }
        }
        data = json.dumps(data, separators=(',', ':'))
        yield feapder.Request(url, data=data, callback=self.parse_url, page=current_page, method='POST',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        # items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(
            response.xpath("//div[@class='newsbody']//p/text()").extract())
        items.author = ''
        items.pubtime = ''
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
