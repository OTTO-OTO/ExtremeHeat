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

    country = 'Uruguay'
    table = 'Uruguay'
    #西班牙语
    keywords = ["Ciclón tropical", "Depresión tropical", "Tormenta tropical", "Tifón", "Huracán", "Ciclón", "Tormenta", "Lluvias intensas", "Inundación", "Marea", "Daños costeros", "Deslizamiento", "Desastre geológico", "Desastre marino", "Vientos fuertes", "Desastre de tifones", "Derrumbes de lodo", "Deslizamiento de tierra"]
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = f"https://www.montevideo.com.uy/anbuscador.aspx?1,1,{keyword},1,,0"
            yield feapder.Request(url, callback=self.parse_url, page=page,
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.montevideo.com.uy/anbuscador.aspx?1,1,Temperatura%20alta,2,,0",
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
            "ARRAffinity": "47507ac2fd4fda18b7d2db4b975a1f2ddc6c1f790edc5b1c841d9094445eeb54",
            "ucrotations_undefined": "Boton1uc914371uc914390uc914392%2C0",
            "___nrbi": "%7B%22firstVisit%22%3A1738982957%2C%22userId%22%3A%22dfe6082d-f5a1-48fc-9949-ec4338eb726e%22%2C%22userVars%22%3A%5B%5D%2C%22futurePreviousVisit%22%3A1738982957%2C%22timesVisited%22%3A1%7D",
            "compass_uid": "dfe6082d-f5a1-48fc-9949-ec4338eb726e",
            "ASP.NET_SessionId": "uxcg44spwt1rioi03spi5stx",
            "GX_SESSION_ID": "zhMyLPur5LwZudXCTCNiQpVdTknTnaDa5gIMqr3%2fnEg%3d",
            "_ga": "GA1.1.561854589.1738982964",
            "_fbp": "fb.2.1738982975942.948671681293092292",
            "panoramaId_expiry": "1739587762361",
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "panoramaId": "dd6967eb7842afd67768334a0c8d16d53938761497b942a43ecff32adb2f1512",
            "cto_bundle": "ufmh-V9rWnZsUVVLSFBGOVdOMkF1JTJGaFJkckxVM2lDN1BaMkFCZ2xnNkxCJTJGanh3WE14Sjg2R3JiJTJGc2sxWTNBSG1nbkJDOU1Hb0pBejJCZUJlWUJ6ZGhkSFJsTnM2JTJGZkZld2wxRlFJdkw4NENMU3dXU0VGelRDNFJmOXdjZktwJTJCSzhORHA",
            "cto_bidid": "EehS-l9OSjJtTTdvZE5CMGZ3WHElMkY0cmRoa3NQQnNkM2VOMVdmMmIweVVZeGNmbTY2NE1KR2V1emlmaDhMNlZ3TTBRRXMybnYlMkZPNWpvTFJvVlR3NnIxayUyRlNlUSUzRCUzRA",
            "cto_dna_bundle": "iT-nT19PT1JQUkJIcFlMNFJhcloxd0pnNGdSZjV4Y3BGUGg5dk5mZU01djluS0glMkJKUzQ4QmYzM2xMTm9rVk9LbEVQaHN6SnJPS0RiVFE2QWprTUFvNFVoRWxBJTNEJTNE",
            "___nrbic": "%7B%22isNewUser%22%3Atrue%2C%22previousVisit%22%3A1738982957%2C%22currentVisitStarted%22%3A1738982957%2C%22sessionId%22%3A%22214ffc1f-2f77-4e37-bb46-c7e0c4175fa0%22%2C%22sessionVars%22%3A%5B%5D%2C%22visitedInThisSession%22%3Atrue%2C%22pagesViewed%22%3A4%2C%22landingPage%22%3A%22https%3A//www.montevideo.com.uy/%22%2C%22referrer%22%3A%22%22%2C%22lpti%22%3Anull%7D",
            "_ga_1CV7BQE5SK": "GS1.1.1738982964.1.1.1738983129.0.0.0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        # print(type(response.text))
        # print(type(response.json))
        # links = json.loads(response.text)
        # print(response.text)
        links = response.xpath("//h2/a/@href").extract()
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
        url = f"https://www.montevideo.com.uy/anbuscador.aspx?1,1,{current_keyword},{current_page},,0"
        yield feapder.Request(url,callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1//text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='contenido']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//div[@itemprop='datePublished']/text()").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
