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

    country = 'Cambodia'
    table = 'Cambodia_rfa'
    keywords = ["热", "极端热浪", "高温", "极端温度", "热浪事件", "高温增加", "高温影响", "高温", "强热", "温度上升",
                "热事件", "温度升高", "强降雨", "强降水", "暴雨", "极端降雨", "干旱", "严重干旱", "长期干旱",
                "水资源短缺", "停电", "高温停电", "热浪停电", "高温导致停电", "火灾", "高温火灾", "热火灾", "温度火灾",
                "高温引发火灾", "农业影响", "热浪农业", "作物损害", "农业热应激", "缺氧", "中暑", "热中暑", "高温缺氧",
                "高温中暑", "交通影响", "高温交通", "热浪交通", "温度交通", "生态灾难", "热灾难", "高温环境",
                "热对生物多样性影响", "热浪生态", "污染", "高温污染", "热污染", "温度污染", "珊瑚白化", "高温珊瑚礁",
                "温度白化珊瑚"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.cna.org.cy/search-results"
            params = {
                "kw": f"{keyword}",
                "pg": "1",
                "sort": "release_date desc"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, method='GET',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Referer": "https://www.cna.org.cy/search-results?kw=heavy%20rain",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        request.cookies = {
            "CookieConsent": "{stamp:%27lkPUiDqtHG1d8IdvjsD5mlGZrr9MbcVoxfZOgb5C+o5kr2Xo/H35qA==%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2Cmethod:%27explicit%27%2Cver:1%2Cutc:1739175545731%2Cregion:%27hk%27}",
            "_gid": "GA1.3.1407162954.1739175573",
            "ASP.NET_SessionId": "bmt2fjxjfsembysq4pw0xfil",
            "_ga_XTM7TC703R": "GS1.1.1739175573.1.1.1739175639.0.0.0",
            "_ga": "GA1.3.1438141215.1739175573"
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
        links = response.xpath("//article/a/@href").extract()

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
            items.article_url = "https://www.rfa.org/" + item.get("link")
            items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.cna.org.cy/search-results"
        params = {
            "kw": f"{current_keyword}",
            "pg": f"{current_page}",
            "sort": "release_date desc"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, method='GET',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        # items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//p[@class='c-paragraph']//text()").extract())
        items.author = ''
        items.pubtime = ''
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
