# -*- coding: utf-8 -*-
# 173
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
        MYSQL_DB="other_country_site1",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Cabo Verde'
    table = 'CaboVerde_expressodasilhas'
    keywords = ["calor","Onda de calor extrema", "Temperatura alta", "Temperatura extrema", "Evento de onda de calor", "Aumento da temperatura alta", "Impacto da temperatura alta", "Temperatura alta", "Calor intenso", "Aumento da temperatura", "Evento de calor", "Aumento da temperatura", "Chuvas intensas", "Precipitação forte", "Chuva torrencial", "Chuvas extremas", "Seca", "Seca severa", "Seca prolongada", "Escassez de água", "Corte de energia", "Corte de energia por alta temperatura", "Corte de energia por onda de calor", "Corte de energia causado por alta temperatura", "Incêndio", "Incêndio por temperatura alta", "Incêndio por calor", "Incêndio provocado por temperatura", "Incêndio induzido por calor", "Impacto agrícola", "Onda de calor na agricultura", "Dano aos cultivos", "Estresse térmico agrícola", "Hipóxia", "Golpe de calor", "Golpe de calor induzido por calor", "Hipóxia por temperatura alta", "Golpe de calor por alta temperatura", "Impacto no tráfego", "Tráfego por alta temperatura", "Tráfego por onda de calor", "Tráfego por temperatura", "Desastre ecológico", "Desastre por calor", "Ambiente de alta temperatura", "Impacto do calor na biodiversidade", "Ecologia da onda de calor", "Poluição", "Poluição por alta temperatura", "Poluição por calor", "Poluição por temperatura", "Desbotamento de corais", "Recifes de corais por alta temperatura", "Desbotamento de corais por temperatura" ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://expressodasilhas.cv/pesquisar/"
            params = {
                "q": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        request.cookies = {
            "connect.sid": "s%3AMklFi5NBMJnChj__wQZScUCyPa5NR-IQ.qNkHpMSQTiIP8QyE1VNYvJ9WGjDJMjOPbXahq8H%2FuxE",
            "_gid": "GA1.2.1155698054.1737337600",
            "_gat_gtag_UA_5208697_2": "1",
            "_ga_JQT6BCL1DW": "GS1.1.1737337600.4.1.1737337744.60.0.0",
            "_ga": "GA1.1.1744261606.1734512252"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")

        links = response.xpath("//div[@class='featured']/div[2]/a/@href").extract()
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
            items.table_name = self.table
            items.article_url = item
            # items.title = item.get("headline")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://expressodasilhas.cv/pesquisar/"
        params = {
            "q": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='articleText']//p/text()").extract())
        items.author = ''
        items.pubtime = json.loads(response.xpath("//script[@type='application/ld+json']/text()").extract_first()).get("datePublished")
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
