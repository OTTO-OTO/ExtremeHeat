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

    country = 'Mozambique'
    table = 'Mozambique_tvm'
    keywords = ["calor","Onda de calor extrema", "Temperatura alta", "Temperatura extrema", "Evento de onda de calor", "Aumento da temperatura alta", "Impacto da temperatura alta", "Temperatura alta", "Calor intenso", "Aumento da temperatura", "Evento de calor", "Aumento da temperatura", "Chuvas intensas", "Precipitação forte", "Chuva torrencial", "Chuvas extremas", "Seca", "Seca severa", "Seca prolongada", "Escassez de água", "Corte de energia", "Corte de energia por alta temperatura", "Corte de energia por onda de calor", "Corte de energia causado por alta temperatura", "Incêndio", "Incêndio por temperatura alta", "Incêndio por calor", "Incêndio provocado por temperatura", "Incêndio induzido por calor", "Impacto agrícola", "Onda de calor na agricultura", "Dano aos cultivos", "Estresse térmico agrícola", "Hipóxia", "Golpe de calor", "Golpe de calor induzido por calor", "Hipóxia por temperatura alta", "Golpe de calor por alta temperatura", "Impacto no tráfego", "Tráfego por alta temperatura", "Tráfego por onda de calor", "Tráfego por temperatura", "Desastre ecológico", "Desastre por calor", "Ambiente de alta temperatura", "Impacto do calor na biodiversidade", "Ecologia da onda de calor", "Poluição", "Poluição por alta temperatura", "Poluição por calor", "Poluição por temperatura", "Desbotamento de corais", "Recifes de corais por alta temperatura", "Desbotamento de corais por temperatura" ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.republikein.com.na/search"
            params = {
                "query": f"{keyword}",
                "pgno": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "content-length": "0",
            "origin": "https://www.republikein.com.na",
            "priority": "u=1, i",
            "referer": "https://www.republikein.com.na/search?query=heavy+rain",
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
            "device": "web",
            "browser_session_cookie": "AISTAeGCzAjjrYn4m8gTK3L8DPRqPc6PoLq3Gq7SiSDhmcgxwxA9o9YvuygWle8eT8bk0umRNRoMg1gdfrvNa2Ex1XGDu9aYHx9IxyoOtHBTHlmJR57CyGumq2jVSNt9",
            "_ga": "GA1.1.1198140512.1738810575",
            "_ga_77TBWENYV7": "GS1.1.1738810574.1.1.1738810705.0.0.0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//h4/a/@href").extract()

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
        url = "https://www.republikein.com.na/search"
        params = {
            "query": f"{current_keyword}",
            "pgno": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='itemFullText']//p//text()").extract())
        items.author = ''
        items.pubtime = ''
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
