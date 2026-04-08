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

    country = 'Chile'
    table = 'Chile_emol'
    keywords = ["calor" ,"Ola de calor extrema", "Temperatura alta", "Temperatura extrema", "Evento de ola de calor", "Aumento de la temperatura alta", "Impacto de la temperatura alta", "Temperatura alta", "Calor intenso", "Aumento de la temperatura", "Evento de calor", "Incremento de la temperatura", "Lluvias intensas", "Precipitación fuerte", "Lluvia torrencial", "Lluvias extremas", "Sequía", "Sequía severa", "Sequía prolongada", "Escasez de agua", "Corte de energía", "Corte de energía por alta temperatura", "Corte de energía por ola de calor", "Corte de energía causado por alta temperatura", "Incendio", "Incendio por temperatura alta", "Incendio por calor", "Incendio provocado por temperatura", "Incendio inducido por calor", "Impacto agrícola", "Ola de calor en la agricultura", "Daño a los cultivos", "Estrés térmico agrícola", "Hipoxia", "Golpe de calor", "Golpe de calor inducido por calor", "Hipoxia por temperatura alta", "Golpe de calor por alta temperatura", "Impacto en el tráfico", "Tráfico por alta temperatura", "Tráfico por ola de calor", "Tráfico por temperatura", "Desastre ecológico", "Desastre por calor", "Entorno de temperatura alta", "Impacto del calor en la biodiversidad", "Ecología de la ola de calor", "Contaminación", "Contaminación por alta temperatura", "Contaminación por calor", "Contaminación por temperatura", "Blanqueamiento de corales", "Arrecifes de coral por alta temperatura", "Blanqueamiento de corales por temperatura" ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://newsapi.ecn.cl/NewsApi/emol/buscador/emol,inversiones,mediosregionales,legal,campo,blogs,guioteca,elmercurio-digital,emoltv,lasegundaprint,revistalibros,mercuriodeportes"
            params = {
                "q": f"{keyword}",
                "size": "10",
                "from": "0"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "origin": "https://www.emol.com",
            "priority": "u=1, i",
            "referer": "https://www.emol.com/",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json['hits'].get("hits")
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
            items.table_name = self.table
            items.article_url = item.get("_source").get("permalink")
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = item.get("_source").get("fechaPublicacion")
            items.content = item.get("_source").get("texto")
            items.title = item.get("_source").get("titulo")
            items.author = ''
            print(items)
            # yield items
            # yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://newsapi.ecn.cl/NewsApi/emol/buscador/emol,inversiones,mediosregionales,legal,campo,blogs,guioteca,elmercurio-digital,emoltv,lasegundaprint,revistalibros,mercuriodeportes"
        params = {
            "q": f"{current_keyword}",
            "size": "10",
            "from": f"{current_page * 10}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    # def parse_detail(self, request, response):
    #     items = request.items
    #     items.title = response.xpath("//h1/text()").extract_first()
    #     items.content = "".join(response.xpath("//article//p/text()").extract())
    #     items.author = ''
    #     items.pubtime = ''
    #     print(items)
    #     if items.content:
    #         yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
