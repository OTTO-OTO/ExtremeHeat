# -*- coding: utf-8 -*-
"""

集群运行

"""
import re
from datetime import datetime

import feapder
from feapder import Item
import json
import time
from lxml import etree
from sympy import print_tree


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[6, 10],
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

    country = 'Uruguay'
    table = 'Uruguay_teledoce'
    keywords = ["calor" ,"Ola de calor extrema", "Temperatura alta", "Temperatura extrema", "Evento de ola de calor", "Aumento de la temperatura alta", "Impacto de la temperatura alta", "Temperatura alta", "Calor intenso", "Aumento de la temperatura", "Evento de calor", "Incremento de la temperatura", "Lluvias intensas", "Precipitación fuerte", "Lluvia torrencial", "Lluvias extremas", "Sequía", "Sequía severa", "Sequía prolongada", "Escasez de agua", "Corte de energía", "Corte de energía por alta temperatura", "Corte de energía por ola de calor", "Corte de energía causado por alta temperatura", "Incendio", "Incendio por temperatura alta", "Incendio por calor", "Incendio provocado por temperatura", "Incendio inducido por calor", "Impacto agrícola", "Ola de calor en la agricultura", "Daño a los cultivos", "Estrés térmico agrícola", "Hipoxia", "Golpe de calor", "Golpe de calor inducido por calor", "Hipoxia por temperatura alta", "Golpe de calor por alta temperatura", "Impacto en el tráfico", "Tráfico por alta temperatura", "Tráfico por ola de calor", "Tráfico por temperatura", "Desastre ecológico", "Desastre por calor", "Entorno de temperatura alta", "Impacto del calor en la biodiversidad", "Ecología de la ola de calor", "Contaminación", "Contaminación por alta temperatura", "Contaminación por calor", "Contaminación por temperatura", "Blanqueamiento de corales", "Arrecifes de coral por alta temperatura", "Blanqueamiento de corales por temperatura" ]
    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://www.teledoce.com//page/3"
            params = {
                "s": "calor"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, method='GET',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=1, i",
            "referer": "https://www.teledoce.com/?s=calor",
            "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Microsoft Edge\";v=\"133\", \"Chromium\";v=\"133\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
            "x-requested-with": "XMLHttpRequest"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//ul//article/a/@href").extract()

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
            # items.title = item.get("title")
            # items.pubtime = item.get("startAt")
            items.article_url = item
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 41
        url = f"https://www.teledoce.com//page/{current_page}"
        params = {
            "s": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, method='GET',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(
            response.xpath("//div[@class='entry-content']//p/text() | //div[@class='article__content']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
