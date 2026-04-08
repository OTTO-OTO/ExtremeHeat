# -*- coding: utf-8 -*-
"""

本地运行

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

    country = 'Syrian Arab Republic'
    table = 'Syrian_Arab_Republic'
    #阿拉伯语
    keywords =  ["عاصفة استوائية", "اضطراب استوائي", "عاصفة استوائية", "إعصار", "إعصار", "إعصار", "عاصفة", "أمطار غزيرة", "فيضان", "ارتفاع", "أضرار ساحلية", "انهيار تربي", "كارثة جيولوجية", "كارثة بحرية", "رياح قوية", "كارثة الإعصار", "انهيار طيني", "انهيار تربي"]
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.syria.tv/views/ajax"
            params = {
                "search_api_fulltext": f"{keyword}",
                "sort_by": "field_published_date",
                "_wrapper_format": "drupal_ajax"
            }
            data = {
                "view_name": "search_custom",
                "view_display_id": "search_page",
                "view_args": "",
                "view_path": "/search-custom",
                "view_base_path": "search-custom",
                "view_dom_id": "96b1811a9a4dd326b1af38a0b42333929d1adb9ccf168af626bf46264962b5d6",
                "pager_element": "0",
                "search_api_fulltext": f"{keyword}",
                "sort_by": "field_published_date",
                "_wrapper_format": "html",
                "page": "1",
                "_drupal_ajax": "1",
                "ajax_page_state%5Btheme%5D": "syriatv",
                "ajax_page_state%5Btheme_token%5D": "",
            }
            yield feapder.Request(url, params=params,data=data, callback=self.parse_url, page=page,
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "authorization": "Basic bmVvOmtsRXdySV9mbzE=",
            "origin": "https://www.nzz.ch",
            "priority": "u=1, i",
            "referer": "https://www.nzz.ch/suche?q=Hitze&filter=none",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        # print(type(response.text))
        # print(type(response.json))
        # links = json.loads(response.text)
        html_str = response.json[-1].get("data")
        html = etree.HTML(html_str)
        links = html.xpath("//h3/a/@href")
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
            items.table_name = self.table  # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url ="https://www.syria.tv" + item
            # items.title = item.get("metadata").get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.syria.tv/views/ajax"
        params = {
            "search_api_fulltext": f"{current_keyword}",
            "sort_by": "field_published_date",
            "_wrapper_format": "drupal_ajax"
        }
        data = {
            "view_name": "search_custom",
            "view_display_id": "search_page",
            "view_args": "",
            "view_path": "/search-custom",
            "view_base_path": "search-custom",
            "view_dom_id": "96b1811a9a4dd326b1af38a0b42333929d1adb9ccf168af626bf46264962b5d6",
            "pager_element": "0",
            "search_api_fulltext": f"{current_keyword}",
            "sort_by": "field_published_date",
            "_wrapper_format": "html",
            "page": f"{current_page}",
            "_drupal_ajax": "1",
            "ajax_page_state%5Btheme%5D": "syriatv",
            "ajax_page_state%5Btheme_token%5D": "",
        }
        yield feapder.Request(url, params=params,data=data, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        # items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//p[@class='text-align-justify']//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
