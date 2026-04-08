# -*- coding: utf-8 -*-
"""

本地运行

"""
import re
from datetime import datetime

import feapder
from feapder import Item
import json
import time
from lxml import etree


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[6, 8],
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

    country = 'South Africa'
    table = 'South_Africa'
    # 英语
    keywords = ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.enca.com/views/ajax"
            params = {
                "search": f"{keyword}",
                "_wrapper_format": "drupal_ajax",
                "view_name": "acquia_search",
                "view_display_id": "page_1",
                "view_args": "",
                "view_path": "/search/news",
                "view_base_path": "search/news",
                "view_dom_id": "52e65e66d7a3a7456316dd1f78a70cc20a83ffe51d8b051271517ff449240665",
                "pager_element": "0",
                "page": "1",
                "_drupal_ajax": "1",
                "ajax_page_state%5Btheme%5D": "enca2023",
                "ajax_page_state[libraries]": ""
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, method='GET',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=1, i",
            "referer": "https://www.enca.com/search/news?search=heavy+rain",
            "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Microsoft Edge\";v=\"133\", \"Chromium\";v=\"133\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
            "x-requested-with": "XMLHttpRequest"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        str_data = response.json[-2].get("data")
        # print(str_data)
        # libraries = response.json[0].get("settings").get("ajaxPageState").get("libraries")
        # print(links)
        links = etree.HTML(str_data).xpath("//a[@class='link']/@href")
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
            items.article_url = "https://www.enca.com" + item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.enca.com/views/ajax"
        params = {
            "search": f"{current_keyword}",
            "_wrapper_format": "drupal_ajax",
            "view_name": "acquia_search",
            "view_display_id": "page_1",
            "view_args": "",
            "view_path": "/search/news",
            "view_base_path": "search/news",
            "view_dom_id": "52e65e66d7a3a7456316dd1f78a70cc20a83ffe51d8b051271517ff449240665",
            "pager_element": "0",
            "page": f"{current_page}",
            "_drupal_ajax": "1",
            "ajax_page_state%5Btheme%5D": "enca2023",
            # "ajax_page_state[libraries]":f"{libraries}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, method='GET',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//meta[@property='og:title']/@content").extract_first()
        items.content = "".join(
            response.xpath("//div[@class='text paragraph--view-mode-default']//p//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
