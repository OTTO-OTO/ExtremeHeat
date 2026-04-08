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
        SPIDER_SLEEP_TIME=[6, 8],
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

    country = 'Antigua and Barbuda'
    table = 'AntiguaandBarbuda_tn'
    keywords = ["calor" ,"Ola de calor extrema", "Temperatura alta", "Temperatura extrema", "Evento de ola de calor", "Aumento de la temperatura alta", "Impacto de la temperatura alta", "Temperatura alta", "Calor intenso", "Aumento de la temperatura", "Evento de calor", "Incremento de la temperatura", "Lluvias intensas", "Precipitación fuerte", "Lluvia torrencial", "Lluvias extremas", "Sequía", "Sequía severa", "Sequía prolongada", "Escasez de agua", "Corte de energía", "Corte de energía por alta temperatura", "Corte de energía por ola de calor", "Corte de energía causado por alta temperatura", "Incendio", "Incendio por temperatura alta", "Incendio por calor", "Incendio provocado por temperatura", "Incendio inducido por calor", "Impacto agrícola", "Ola de calor en la agricultura", "Daño a los cultivos", "Estrés térmico agrícola", "Hipoxia", "Golpe de calor", "Golpe de calor inducido por calor", "Hipoxia por temperatura alta", "Golpe de calor por alta temperatura", "Impacto en el tráfico", "Tráfico por alta temperatura", "Tráfico por ola de calor", "Tráfico por temperatura", "Desastre ecológico", "Desastre por calor", "Entorno de temperatura alta", "Impacto del calor en la biodiversidad", "Ecología de la ola de calor", "Contaminación", "Contaminación por alta temperatura", "Contaminación por calor", "Contaminación por temperatura", "Blanqueamiento de corales", "Arrecifes de coral por alta temperatura", "Blanqueamiento de corales por temperatura" ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://tn.com.ar/pf/api/v3/content/fetch/search-api"
            keyword = keyword.replace(" ", "-")
            params = {
                "query": f"{{\"feed_size\":20,\"page\":2,\"query\":\"{keyword}\",\"size\":20}}",
                "filter": "{content_elements{credits{by{_id,name,type}},description{basic},display_date,headlines{basic,mobile},promo_items{basic{_id,additional_properties{focal_point{max,min}},auth{1},caption,content_elements{_id,auth{1},caption,focal_point{x,y},url},embed{config{auth{1},created,is_live_content,thumbnail},id},focal_point{x,y},subtype,type,url},cover{_id,additional_properties{focal_point{max,min}},auth{1},caption,content_elements{_id,auth{1},caption,focal_point{x,y},url},embed{config{auth{1},created,is_live_content,thumbnail},id},focal_point{x,y},subtype,type,url}},subheadlines{basic},taxonomy{tags{text}},type,website_url},count,next}",
                "d": "565",
                "mxId": "00000000",
                "_website": "tn"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page,method='GET',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=1, i",
            "referer": "https://tn.com.ar/buscar/Temperatura-alta/",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0"
        }
        request.cookies = {
            "_gcl_au": "1.1.1963331655.1739004974",
            "first_visit_cookie": "1",
            "_scor_uid": "f0dbd28da9054d9fb90314807b0d97ad",
            "compass_uid": "131e5d27-fc1a-4446-abde-c35f2b2e4dfa",
            "lux_uid": "173914771589670102",
            "_gid": "GA1.3.1958656814.1739147718",
            "___nrbi": "%7B%22firstVisit%22%3A1739004976%2C%22userId%22%3A%22131e5d27-fc1a-4446-abde-c35f2b2e4dfa%22%2C%22userVars%22%3A%5B%5B%22mrfExperiment_AB%22%2C%221%22%5D%5D%2C%22futurePreviousVisit%22%3A1739147718%2C%22timesVisited%22%3A2%7D",
            "_ga": "GA1.3.1645063518.1739004974",
            "_ga_BF24CG4BRZ": "GS1.1.1739147718.2.1.1739147835.60.0.0",
            "___nrbic": "%7B%22previousVisit%22%3A1739004976%2C%22currentVisitStarted%22%3A1739147718%2C%22sessionId%22%3A%22cf1a4ea2-a614-4d18-813d-fb2e9deacfbb%22%2C%22sessionVars%22%3A%5B%5D%2C%22visitedInThisSession%22%3Atrue%2C%22pagesViewed%22%3A2%2C%22landingPage%22%3A%22https%3A//tn.com.ar/%22%2C%22referrer%22%3A%22%22%2C%22lpti%22%3A%222025-02-09T21%3A30%3A25.325Z%22%7D"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        # print(type(response.text))
        # print(type(response.json))
        # links = json.loads(response.text)
        # print(response.text)
        # print(response.text)
        # print(response)
        links = response.json['content_elements']
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
            items.article_url = "https://tn.com.ar/" + item.get("website_url")
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://tn.com.ar/pf/api/v3/content/fetch/search-api"
        params = {
            "query": f"{{\"feed_size\":20,\"page\":{current_page},\"query\":\"{current_page}\",\"size\":20}}",
            "filter": "{content_elements{credits{by{_id,name,type}},description{basic},display_date,headlines{basic,mobile},promo_items{basic{_id,additional_properties{focal_point{max,min}},auth{1},caption,content_elements{_id,auth{1},caption,focal_point{x,y},url},embed{config{auth{1},created,is_live_content,thumbnail},id},focal_point{x,y},subtype,type,url},cover{_id,additional_properties{focal_point{max,min}},auth{1},caption,content_elements{_id,auth{1},caption,focal_point{x,y},url},embed{config{auth{1},created,is_live_content,thumbnail},id},focal_point{x,y},subtype,type,url}},subheadlines{basic},taxonomy{tags{text}},type,website_url},count,next}",
            "d": "565",
            "mxId": "00000000",
            "_website": "tn"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,method='GET',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//meta[@name='og:title']/@content").extract_first()
        items.content = "".join(response.xpath("//div[@class='raw_container']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
