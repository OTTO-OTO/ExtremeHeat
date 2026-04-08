import json
import re

import feapder
from NewsItems import SpiderDataItem
from curl_cffi import requests
from lxml import etree


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[6, 8],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
    )

    country = 'Israel'
    table = 'Israel_haaretz'
    keywords = ['heavy rain']

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://heyday.io/search/s/"
            data = {
                "affId": "2409",
                "h": "www.haaretz.com",
                "q": f"{keyword}",
                "p": 1
            }
            data = json.dumps(data, separators=(',', ':'))
            yield feapder.Request(url, data=data, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Content-Type": "application/json",
            "Origin": "https://www.haaretz.com",
            "Referer": "https://www.haaretz.com/search-results?q=heavy+rain",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "cross-site",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
            "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Microsoft Edge\";v=\"133\", \"Chromium\";v=\"133\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json['r']
        # print(links)
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            # print(item,"---------------")
            # print(item.get("title"))
            items = SpiderDataItem()
            items.article_url = item.get("pd").get("url")
            items.title = item.get("pd").get("title")
            items.pubtime = item.get("pd").get("publishTime")
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://heyday.io/search/s/"
        data = {
            "affId": "2409",
            "h": "www.haaretz.com",
            "q": f"{current_keyword}",
            "p": current_page
        }
        data = json.dumps(data, separators=(',', ':'))
        yield feapder.Request(url, data=data, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        # print(response.text)
        items = request.items
        items.table_name = self.table
        # items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//section[@itemprop='articleBody']//p/text()").extract())
        items.author = ''
        items.pubtime = ''
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
