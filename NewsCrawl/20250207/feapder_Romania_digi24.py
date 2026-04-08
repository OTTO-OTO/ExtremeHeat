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

    country = 'Romania'
    table = 'Romania'
    #罗马尼亚语
    keywords = ["Ciclon tropical", "Depresiune tropicala", "Furtuna tropicala", "Tifon", "Uragan", "Ciclon", "Furtuna", "Ploi torentiale", "Inundatie", "Val de apa", "Daune coastale", "Alunecare", "Dezastre geologice", "Dezastre maritime", "Vanturi puternice", "Dezastre provocate de tifon", "Alunecare de teren", "Alunecare de pamant"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.digi24.ro/cautare"
            params = {
                "q": f"{keyword}",
                "ps": "10",
                "p": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.digi24.ro/cautare?q=temperaturi+extreme",
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
            "_pctx": "%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAEzIEYOBmANgCZ%2BAdgCcQ-gA5eAVhHDuI3iAC%2BQA",
            "_pcid": "%7B%22browserId%22%3A%22m6u1m498f7mw9zc5%22%7D",
            "cX_P": "m6u1m498f7mw9zc5",
            "cX_G": "cx%3A1brxzhtu7kht937bk5xs1dwe6d%3Ago5y5i1lzt6e",
            "OptanonAlertBoxClosed": "2025-02-07T00:48:10.570Z",
            "eupubconsent-v2": "CQMcJ0AQMcJ0AAcABBROBbF8AP_gAAAAAChQKfwKAABAAQgBOAFAALAAZABEACNAE0ATgAoABUAC2AF0AL8AYQBiADwAH4AeYBCACIgEWAKQAXUA6gB9gEMAI4AW2AvMBlgDdQILAT0gn-CgcFBgUTAorBRcFGYKQApXBS0FMgKaQU2BT8AAABISAKADIAIgAfgF5gN1HQCwAZABEAC6AGIAPwBFgDqAH2AXmAywBuo4AIAQgAiIDjwH2kIAQAYgDqEoAgARAAxAHUAXmSAAgMsKQCgAZABEADEAH4AiwB1AD7ALzAZYA3UoABAfaWgAgH2A.f_wAAAAAAAAA",
            "cmp_level": "15",
            "m2digi24ro": "0",
            "_ga": "GA1.2.17883064.1738889335",
            "_gid": "GA1.2.1334653797.1738889335",
            "_gat_UA-29367039-1": "1",
            "cebs": "1",
            "_ce.clock_data": "20542%2C185.18.222.168%2C1%2C40b65ea82f99d9ae2d2769173a01ce1b%2CEdge%2CJP",
            "cebsp_": "1",
            "_ga_1RJCTC40WK": "GS1.1.1738889335.1.0.1738889342.0.0.0",
            "OptanonConsent": "isGpcEnabled=0&datestamp=Fri+Feb+07+2025+08%3A49%3A03+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=06707c59-13a7-4862-ba60-97dd30943a72&interactionCount=2&isAnonUser=1&landingPath=NotLandingPage&AwaitingReconsent=false&groups=C0010%3A1%2CBG2435%3A1%2CC0001%3A1%2CC0008%3A1%2CC0003%3A1%2CC0002%3A1%2CC0004%3A1%2CC0009%3A1%2CC0011%3A1%2CC0007%3A1%2CC0005%3A1%2CV2STACK1%3A1&intType=1&geolocation=%3B",
            "_ce.s": "v~59e1475e1325d867d238f5659dc8c1467776d811~lcw~1738889352413~vir~new~lva~1738889337151~vpv~0~v11.fhb~1738889338528~v11.lhb~1738889338528~v11.cs~405520~v11.s~50c5ff30-e4ed-11ef-a196-8d7e22da2c40~v11.sla~1738889352413~v11.send~1738889352478~lcw~1738889352478"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//article//h2/a/@href").extract()
        # print(links)
        current_page = request.page

        current_links = links
        if self.previous_links is not None and current_links == self.previous_links:
            print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理

        self.previous_links = current_links  # 更新上一页的链接列表
        if request.page >= 10:
            print(f"关键词 {current_keyword} 的第 {request.page} 页已经抓取完毕，退出当前关键字的循环")
            return None
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
        url = "https://www.digi24.ro/cautare"
        params = {
            "q": f"{current_keyword}",
            "ps": "10",
            "p": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(
            response.xpath("//div[@class='entry data-app-meta data-app-meta-article']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='publish-date']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
