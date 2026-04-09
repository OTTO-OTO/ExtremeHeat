# -*- coding: utf-8 -*-
import json
import re
import time
import uuid

import feapder
from feapder import Item


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
    country = 'Peru'
    table = 'Peru'
    # 西班牙语
    keywords = ["Ciclón tropical", "Depresión tropical", "Tormenta tropical", "Tifón", "Huracán", "Ciclón", "Tormenta", "Lluvias intensas", "Inundación", "Marea", "Daños costeros", "Deslizamiento", "Desastre geológico", "Desastre marino", "Vientos fuertes", "Desastre de tifones", "Derrumbes de lodo", "Deslizamiento de tierra"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://rpp.pe/buscar"
            data = {
                "texto": f"{keyword}",
                "filtrar": "todos"
            }
            yield feapder.Request(url, data=data, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "cache-control": "max-age=0",
            "content-type": "application/x-www-form-urlencoded",
            "origin": "https://rpp.pe",
            "priority": "u=0, i",
            "referer": "https://rpp.pe/buscar",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "_ga": "GA1.1.2115214949.1737101080",
            "compass_uid": "dfd93cf4-5652-4a57-903a-23e18fb19876",
            "___nrbi": "%7B%22firstVisit%22%3A1737101080%2C%22userId%22%3A%22dfd93cf4-5652-4a57-903a-23e18fb19876%22%2C%22userVars%22%3A%5B%5B%22mrfExperiment_Lo%20m%E1s%20le%EDdo%22%2C%222%22%5D%5D%2C%22futurePreviousVisit%22%3A1737101080%2C%22timesVisited%22%3A1%7D",
            "FPID": "FPID2.2.3XWu1IHvT8OdnNEFaF9rKXR7ioT98rZGD2ZWw7jCL7A%3D.1737101080",
            "FPLC": "yTX3mC7j9dBvacQN7eu%2BuG5stqsURVE0hVQGOJYn51qKjXvS7hOtHJI2rRhfx7FYcovrLUoJFVkTwnxIizQVDIdC2Mh7zYDKEndisDsUp9ZxXpLgHZWNwnFCGG105w%3D%3D",
            "_scor_uid": "a007103d79a5434b931319c50f009105",
            "GN_USER_ID_KEY": "8ff6b630-e786-4d7d-89a8-534e69f83377",
            "GN_SESSION_ID_KEY": "0453a521-834e-498a-9634-fe24c58afac7",
            "_fbp": "fb.1.1737101091599.995642161754524476",
            "_gcl_au": "1.1.1930070743.1737101092",
            "___nrbic": "%7B%22isNewUser%22%3Atrue%2C%22previousVisit%22%3A1737101080%2C%22currentVisitStarted%22%3A1737101080%2C%22sessionId%22%3A%22fbf2e2b1-d27f-4882-a5b9-8fd9c7d1ea22%22%2C%22sessionVars%22%3A%5B%5D%2C%22visitedInThisSession%22%3Atrue%2C%22pagesViewed%22%3A3%2C%22landingPage%22%3A%22https%3A//rpp.pe/%22%2C%22referrer%22%3A%22%22%2C%22lpti%22%3Anull%7D",
            "_tfpvi": "NzQ0ZWQ3ODgtYzBjNy00NGU4LTlmOTgtYTg3ODFlMGQ1Y2QyIzYtMg%3D%3D",
            "gravitecOptInBlocked": "true",
            "_ga_VQCCX98QVG": "GS1.1.1737101079.1.1.1737101234.0.0.937885861"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//article//h2[@class='news__title']/a/@href").extract()
        # print(json.loads(links))
        # # 输出匹配的值
        # for match in matches:
        #     print(match)

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
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            # items.pubtime = item.get("recs-publishtime")
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        # current_page = request.page + 1
        # url = "https://rpp.pe/buscar"
        # data = {
        #     "texto": "",
        #     "filtrar": "todos"
        # }
        # yield feapder.Request(url, data=data, callback=self.parse_url, page=current_page, keyword=current_keyword,
        #                       filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='body']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
