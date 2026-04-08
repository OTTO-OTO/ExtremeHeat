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

    country = 'Bosnia and Herzegovina'
    table = 'BosniaandHerzegovina_klix'
    keywords = [
    "toplina", "ekstremna toplina", "visoka temperatura", "ekstremna temperatura", "toplinska vala", "povećanje temperature", "utjecaj temperature", "visoka temperatura", "jaka toplina", "porast temperature", "toplinska događanja", "porast temperature", "intenzivna oborina", "intenzivna padavina", "ljeđa", "ekstremna oborina", "suša", "teška suša", "duga suša", "manjak vode", "isključenje", "isključenje zbog temperature", "toplinska vala isključenje", "isključenje zbog temperature", "požar", "požar zbog temperature", "toplinski požar", "požar zbog temperature", "požar uzrokovao temperaturom", "utjecaj na poljoprivredu", "toplinska vala u poljoprivredi", "štetica biljaka", "toplični stres u poljoprivredi", "hipoksija", "toplinski udar", "toplinski udar", "hipoksija zbog temperature", "toplični udar", "utjecaj na promet", "promet u toplini", "toplinska vala u prometu", "temperatura u prometu", "ekološka katastrofa", "toplinska katastrofa", "toplinski okruženje", "utjecaj topline na biološku raznolikost", "toplinska vala u ekologiji", "zagađenje", "zagađenje zbog temperature", "toplinska zagađenja", "zagađenje temperature", "blijedanje korala", "toplinska vala na korali", "blijedanje korala zbog temperature"
]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.klix.ba/pretraga"
            params = {
                "str": "1",
                "q": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, method='GET',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.klix.ba/pretraga?q=visoka+temperatura%22",
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
            "theme": "light",
            "_ga": "GA1.1.191852674.1739170716",
            "DotMetrics.DomainCookie": "{\"dc\":\"c6972726-15d9-48c1-826d-8a95a334c455\",\"ts\":1739170716855}",
            "DM_SitId287": "1",
            "DM_SitId287SecId1288": "1",
            "_lr_geo_location": "HK",
            "XSRF-TOKEN": "eyJpdiI6IlkyWVYxMFU1MWRUazBPeHBsY1VPV3c9PSIsInZhbHVlIjoiYVRCTFV6QmRmMGpQNVZadnloOXp0ejgxSE5vWWlmUUdkcVBTSXJjVm5XcVNQSHI3aWd2QXFHZk4vYTZiRFk4Y045UXVMbk5aMXRjSzR5UnpEajhNUG95RVV0ZmlId2ZqOW01aTBsbjFNK0Z4clhCeThheXZGTzVtMWF2VE9jaWwiLCJtYWMiOiI2ZmU1OWRhOGZmNDQxOTc5ZjU2MjliYTcwMGRmOTZlOWFjNTIyYTk3ZThkMGFkZDQyZWNiMWJkNGI2YWRjOTU1IiwidGFnIjoiIn0%3D",
            "klixba_session": "eyJpdiI6IkFIeHZYUGJwYjZMRHJnSnRnaCs2YkE9PSIsInZhbHVlIjoieEJjb0RiWHVNMC9VcGN5WkRkcVlMdk5aYnRFaUR2d0NDREttOGVwNThoWGROaEYyVTZ0RzR2TjlmZnhpc3RNR0NrRVZkeXZYbGRiUHhkWFBXcit2eVAwcDNhd2tJTzljYS9RMWdlZXlmYXp4TTh0ejNqdjE3RFF1OXM5MW1UNzQiLCJtYWMiOiI0ZDA2YWE1MWY0YTA0NWJiN2U4ODE2MWJmZTRkODdkN2NjY2NkYTc4NWUwMmJhNWM4NzJjNWZmNjdjMzhlYmI3IiwidGFnIjoiIn0%3D",
            "DM_SitId287SecId1299": "1",
            "cto_bundle": "FHUz4l9PT1JQUkJIcFlMNFJhcloxd0pnNGdkeUZKMXg1NHpxN0dhREFUMjBBejlCb2NrNkEyWHkyRjdJMHZIQ1hvdEtTbnFuaXZyR05SSVZGa2lOOWRtcVJwNnlndHNzRUhSdGRIQjN5dVJGJTJGVU1LV3hUd3BDcU9xb2x5SjhMSWdaTjFiN3NGb2dSTWZhRVY4bDBaSHN4SEZ6dyUzRCUzRA",
            "cto_bidid": "cnn-rF9sVkJndyUyQlplSlcwb3I5YjdNRXJPQ1lWUVlnWHdlTUdNTCUyQk9hWm9JQ2VKUzNidVZ5d3oyN09ZJTJCbG9ObWVVTElmenI2N1dsUUNFTGJqV2JMalpFV2hIWXVuZUNqSllsYU9FRm1PUU1CYm9sSSUzRA",
            "cto_dna_bundle": "fbrLOF9PT1JQUkJIcFlMNFJhcloxd0pnNGdSZjV4Y3BGUGg5dk5mZU01djluS0glMkJKUzQ4QmYzM2xMTm9rVk9LbEVQaHM4OGo4JTJCenBjQ3M4ZEYyMFZ4TEM5WHclM0QlM0Q",
            "_ga_C1R3FZH249": "GS1.1.1739170715.1.1.1739170852.31.0.529490564"
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
        links = response.xpath("//article//a[@class='hover:text-gray-600']/@href").extract()
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
        url = "https://www.klix.ba/pretraga"
        params = {
            "str": f"{current_page}",
            "q": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, method='GET',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@id='tekst']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@name='publish-date']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
