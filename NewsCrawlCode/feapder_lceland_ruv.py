import json
import re

import feapder
from NewsItems import SpiderDataItem
from curl_cffi import requests
from lxml import etree

class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(
        SPIDER_THREAD_COUNT = 5,  # 爬虫并发数，追求速度推荐32
        SPIDER_SLEEP_TIME = [5, 8],
        SPIDER_MAX_RETRY_TIMES = 1,  # 每个请求最大重试次数
    )

    country = 'Iceland'
    table = 'Iceland'
    keywords = [ "Extreme heatwave", "High temperature", "Extreme temperature", "Heatwave event", "Increase in high temperature", "High temperature impact", "High temperature", "Strong heat", "Temperature rise", "Heat event", "Temperature increase", "Heavy rainfall", "Heavy precipitation", "Torrential rain", "Extreme rainfall", "Drought", "Severe drought", "Long-term drought", "Water shortage", "Power outage", "High temperature power outage", "Heatwave power outage", "Power outage caused by high temperature", "Fire", "High temperature fire", "Heat fire", "Temperature fire", "Heat-induced fire", "Agricultural impact", "Heatwave agriculture", "Crop damage", "Agricultural heat stress", "Hypoxia", "Heatstroke", "Heat-induced heatstroke", "High temperature hypoxia", "High temperature heatstroke", "Traffic impact", "High temperature traffic", "Heatwave traffic", "Temperature traffic", "Ecological disaster", "Heat disaster", "High temperature environment", "Impact of heat on biodiversity", "Heatwave ecology", "Pollution", "High temperature pollution", "Heat pollution", "Temperature pollution", "Coral bleaching", "High temperature coral reefs", "Temperature coral bleaching" ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://gql.nyr.ruv.is/search/"
            params = {
                "search": f"{keyword}",
                "filter": "0"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page,keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "sec-ch-ua-platform": "\"Windows\"",
            "Referer": "https://www.ruv.is/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            print(item)
            items = SpiderDataItem()
            items.table_name = self.table
            items.article_url = item.get("url")
            items.title = item.get("title")
            items.country = self.country
            items.author = ''
            items.pubtime = ''
            items.keyword = request.keyword # 确保 items.keyword 被正确赋值
            if item.get("body"):
                content = item.get("body")
                data = json.loads(content)[0].get("value")
                html = etree.HTML(data)
                items.content = "".join(html.xpath("//p/text()"))
                print(items)
                yield items

            # yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)


    # def parse_detail(self, request, response):
    #     items = request.items
    #     items.table_name = self.table
    #     items.title = response.xpath("//h1/text()").extract_first()
    #     items.content =
    #     items.author = ''
    #     items.pubtime = response.xpath("//time/@datetime").extract_first()
    #     print(items)
    #     if items.content:
    #         yield items

if __name__ == "__main__":
    AirSpiderDemo().start()
