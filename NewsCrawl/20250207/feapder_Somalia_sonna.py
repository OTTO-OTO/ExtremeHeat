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

    country = 'Somalia'
    table = 'Somalia'
    # 英语
    keywords = ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://sonna.so/en/page/1/"
            params = {
                "s": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page,
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://siol.net/isci/?query=toplot",
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
            "csrftoken": "JgGKzix96rJQmFKOhVfIKIfplRwlkY9ZOEjAn1EGuilWFkqdG7CSo9QVJJhmal2c",
            "_gcl_au": "1.1.343904655.1738899405",
            "_ga": "GA1.1.1907377900.1738899405",
            "_fbp": "fb.1.1738899405149.548109911554094653",
            "DotMetrics.DomainCookie": "{\"dc\":\"304990a3-35e7-4688-b6da-f20cf150d85b\",\"ts\":1738899405496}",
            "DM_SitId457": "1",
            "DM_SitId457SecId1831": "1",
            "_oid": "f9c53528-4ad0-4fcc-a2a0-5c3f576c7fe6",
            "panoramaId_expiry": "1739504187182",
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "panoramaId": "dd6967eb7842afd67768334a0c8d16d53938761497b942a43ecff32adb2f1512",
            "DotMetrics.DomainCookieStress": "{\"dc\":\"bf25fa69-0003-4ac6-9395-1d3b273a416f\",\"ts\":1738899407867}",
            "DM_SitId457Stress": "1",
            "DM_SitId457SecId1831Stress": "1",
            "_ga_6NPY2EFBJD": "GS1.1.1738899409.1.0.1738899773.0.0.0",
            "DM_SitId457SecId4506": "1",
            "DM_SitId457SecId4506Stress": "1",
            "cto_bundle": "qQrUll9PT1JQUkJIcFlMNFJhcloxd0pnNGdVQzRwRjIlMkZTJTJCdVE1T0cyY3hsTmt3dE00bW54SlclMkJ4OTNtR2pPa2J1dnpJandXWGlmJTJGWXlTQVVQU2lNaXBwd3hZVnVQYmVuJTJCaXBhTDEyUnFheTd0UDdxckVvNEtaUnU3NlZzd0prRUIxQWtaRGZ1NFJYOWt6RzVNWlFoSzFseWp3JTNEJTNE",
            "cto_bidid": "X6gb619SNzJieGVIV1AlMkJmWkNPMGxONmUlMkZobElzQ2JrSmphZzRGTWhnckljV2Zkb0ZkbXh2VXdWb0VqVksxYmpQeXVWTllBVk1BbTB0UGhjVGNnSG9LMlJiTm0yT3pPQWpSajRUdklqekNCUVdISXclM0Q",
            "cto_dna_bundle": "tg8b3l9PT1JQUkJIcFlMNFJhcloxd0pnNGdSZjV4Y3BGUGg5dk5mZU01djluS0glMkJKUzQ4QmYzM2xMTm9rVk9LbEVQaHN2cmFUaVVNV2plclQ0aEtKZzdkaEJBJTNEJTNE",
            "_ga_TM4X2QRRC0": "GS1.1.1738899404.1.1.1738899947.42.0.417917430"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        # print(type(response.text))
        # print(type(response.json))
        # links = json.loads(response.text)
        links = response.xpath("//h3/a/@href").extract()
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
        url = f"https://sonna.so/en/page/{current_page}/"
        params = {
            "s": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(
            response.xpath("//div[@class='entry-content rbct clearfix is-highlight-shares']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
