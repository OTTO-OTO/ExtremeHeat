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
    country = 'Panama'
    table = 'Panama'
    keywords = ["Ciclón tropical", "Depresión tropical", "Tormenta tropical", "Tifón", "Huracán", "Ciclón", "Tormenta", "Lluvias intensas", "Inundación", "Marea", "Daños costeros", "Deslizamiento", "Desastre geológico", "Desastre marino", "Vientos fuertes", "Desastre de tifones", "Derrumbes de lodo", "Deslizamiento de tierra"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.adressa.no/client-api/search-results/adresseavisen/"
            params = {
                "q": f"{keyword}",
                "size": "25",
                "page": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=1, i",
            "referer": "https://www.adressa.no/sok/?q=varme",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "abx.v1": "14",
            "lastGdprInfo": "14:54:50 GMT+0800",
            "_k5a": "61@{\"u\":[{\"uid\":\"rsQFH0Qk5WtrQPUD\",\"ts\":1737096892},1737186892]}",
            "consentUUID": "5224cde0-1995-41ca-916e-747176c3e2d3",
            "cis-jwe": "eyJpc3N1ZWRBdCI6IjIwMjUtMDEtMTdUMDc6MTc6MTFaIiwiZW5jIjoiQTEyOENCQy1IUzI1NiIsInJlSXNzdWVkQXQiOiIyMDI1LTAxLTE3VDA3OjE3OjExWiIsImFsZyI6ImRpciIsImtpZCI6IjIifQ..Xon5eYPyLW4B9P0p21Z8Mw.xu-hM148-9WGRVj62XWoSiu9ZmmG-_1NdboFpCZMskkBAsr621e3gPbNnX9BKlNIK_g_Q-EVxyFPqeFJt9YqWg.vQ4YdHOjp_ECpCVUTzEDhA",
            "_pulse2data": "df5829e1-3506-488f-a254-a527be3cd706%2Cv%2C%2C1737703031000%2CeyJpc3N1ZWRBdCI6IjIwMjUtMDEtMTdUMDc6MTc6MTFaIiwiZW5jIjoiQTEyOENCQy1IUzI1NiIsInJlSXNzdWVkQXQiOiIyMDI1LTAxLTE3VDA3OjE3OjExWiIsImFsZyI6ImRpciIsImtpZCI6IjIifQ..YRhQZh2cf8-pJ8lTm77iaA.udRAeWRE5LADEizLLCIZx-BgHwczEt2Hl4r-dFHRyCoS-m9n_LGK8P7JhtoOWNeVwgD0sRnD2dfBt58ZUCK-WmPmzIDH_0hOtqUtAOG1KZa2Avi8Y1uNBZpiGF8NmGSBpDsvwFus1Z-wOrSevRflxhmtgMZ2KkeoKcEuoLPaz6MG-8s99kpwMeuzIXj5BmU6Vc-g4C22XCgQ-5AeXtfVY3iVwd8bVMMU3UA6du2GI-Y.c1ebv20hXHWv6K1kRsUNkw%2C%2C0%2Ctrue%2C%2C",
            "_pulsesession": "%5B%22sdrn%3Aschibsted%3Asession%3A5c30188b-6a99-4515-b3a1-7884c9230a3c%22%2C1737096895772%2C1737098278746%5D"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json['items']
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
            items.article_url ="https://www.adressa.no" + item.get("urls").get("relative")
            # items.title = item.get("title")
            items.country = self.country
            # items.pubtime = item.get("recs-publishtime")
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.adressa.no/client-api/search-results/adresseavisen/"
        params = {
            "q": f"{current_keyword}",
            "size": "25",
            "page": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='article-body-wrapper']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
