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
        SPIDER_SLEEP_TIME=[5, 8],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
    )

    country = 'lceland'
    table = 'lceland_ruv'
    keywords =  [
    'Einkennd', 'Hit', 'Há tempraturn', 'Þungl', 'Þurrka',
    'Stromstöðv af hiti', 'Eld', 'Loftsvernd', 'Klímabreyting',
    'Minnka á vexti', 'Eyðingur af oksi', 'Há tempraturn áhrif á ferðalag',
    'Eðlileg katastrofi', 'Klímabreyting áhrif á efnahagsstarf', 'Sjórvarmar',
    'Há tempraturn forurðun', 'Koral'
]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://gql.nyr.ruv.is/search/"
            params = {
                "search": f"{keyword}",
                "filter": "0"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

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
            # print(item.get("title"))
            items = SpiderDataItem()
            items.table_name = self.table
            items.article_url = item.get("url")
            items.title = item.get("title")
            items.country = self.country
            try:
                body = json.loads(item.get("body",''))[0]
            except Exception as e:
                print(e)
                continue
            html = etree.HTML(body.get("value"))
            content = "".join(html.xpath("//p//text()"))
            items.content = content
            items.keyword = current_keyword
            items.pubtime = ''
            print(items)
            if content:
                yield items
            # items.keyword = current_keyword
            # yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        # current_page = request.page + 1
        # url = "https://gql.nyr.ruv.is/search/"
        # params = {
        #     "search": f"{current_keyword}",
        #     "filter": "0"
        # }
        # yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    # def parse_detail(self, request, response):
    #     # print(response.text)
    #     items = request.items
    #     items.table_name = self.table
    #     # items.title = response.xpath("//h1/text()").extract_first()
    #     items.content = "".join(response.xpath("//div[@class='cikk-torzs']//p/text()").extract())
    #     items.author = ''
    #     items.pubtime = ''
    #     print(items)
    #     if items.content:
    #         yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
