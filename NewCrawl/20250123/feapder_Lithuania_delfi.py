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

    country = 'Lithuania'
    table = 'Lithuania_delfi'
    keywords = [
    "karštas", "ekstremali karščio banga", "aukšta temperatūra", "ekstremalios temperatūros", "karščio bangos įvykiai",
    "aukštų temperatūrų padidėjimas", "aukštų temperatūrų poveikis", "aukšta temperatūra", "stiprus karštis", "temperatūros kilimas",
    "karščio įvykiai", "temperatūros kilimas", "stiprus lietus", "stiprus lietavimas", "lietukas", "ekstremalus lietavimas",
    "drausma", "sunkus drausmas", "ilgalaikis drausmas", "vandens stoka", "energijos nujoda", "karščio dėl energijos nujodos",
    "karščio bangos dėl energijos nujodos", "karščio dėl energijos nujodos", "gaisras", "karščio gaisras", "karščio gaisras",
    "temperatūros gaisras", "karščio dėl gaisro", "žemės ūkio poveikis", "karščio bangos žemės ūkio", "sodybinė žala",
    "žemės ūkio karščio stresas", "hipoksija", "šiltnamio", "karščio hipoksija", "karščio šiltnamio", "transporto poveikis",
    "karščio dėl transporto", "karščio bangos dėl transporto", "temperatūros dėl transporto", "ekologinis kataklizmas",
    "karščio kataklizmas", "karščio aplinkos poveikis", "karščio poveikis biologinei įvairovei", "karščio bangos ekologija",
    "teršimas", "karščio teršimas", "karščio teršimas", "temperatūros teršimas", "koralų baltimas", "karščio koralų rifai",
    "temperatūros baltimas koralų"
]
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://content.api.delfi.lt/content/v3/graphql"
            params = {
                "operationName": "portal_root_getUniversalHeadlines",
                "variables": f'{{"orderBy":"RELEVANCE","orderOptions":null,"getCount":true,"getViews":false,"lite":false,"search":{{"field":["AUTHOR"],"value":"{keyword}"}},"excludeChannels":["www.delfi.lt/ru","www.delfi.lt/en","www.delfi.lt/elta","www.delfi.lt/fone"],"publishAt":{{"from":null,"to":null}},"offset":0,"limit":40,"excludeCategories":[72287270,146,125]}}',
                "extensions": '{"persistedQuery":{"version":1,"sha256Hash":"489040c65c213eacdd884703f39bcfa1a19ffbad4626814bebea4ee3d0e897b3"}}'
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "sec-ch-ua-platform": "\"Windows\"",
            "Referer": "https://www.delfi.lt/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "accept": "*/*",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "content-type": "application/json",
            "sec-ch-ua-mobile": "?0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json['data'].get("headlines").get("items")

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
            items.article_url = "https://www.delfi.lt/verslas/verslas/" + item.get("slug") + "-" + str(item.get("id"))
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 40
        url = "https://content.api.delfi.lt/content/v3/graphql"
        params = {
            "operationName": "portal_root_getUniversalHeadlines",
            "variables": f'{{"orderBy":"RELEVANCE","orderOptions":null,"getCount":true,"getViews":false,"lite":false,"search":{{"field":["AUTHOR"],"value":"{current_keyword}"}},"excludeChannels":["www.delfi.lt/ru","www.delfi.lt/en","www.delfi.lt/elta","www.delfi.lt/fone"],"publishAt":{{"from":null,"to":null}},"offset":{current_page},"limit":40,"excludeCategories":[72287270,146,125]}}',
            "extensions": '{"persistedQuery":{"version":1,"sha256Hash":"489040c65c213eacdd884703f39bcfa1a19ffbad4626814bebea4ee3d0e897b3"}}'
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.content = "".join(response.xpath("//div[@class='row justify-content-center flex-column article__body']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@name='cXenseParse:recs:publishtime']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
