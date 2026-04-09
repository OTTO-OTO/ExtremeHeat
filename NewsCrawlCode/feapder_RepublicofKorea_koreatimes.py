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

    country = 'Republic of Korea'
    table = 'Republic_of_Korea'
    #英语
    keywords =["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.koreatimes.co.kr/www2/common/pages/ajax_search.asp"
            params = {
                "kwd": f"{keyword}",
                "pageNum": "1",
                "pageSize": "10",
                "category": "TOTAL",
                "sort": "",
                "startDate": "",
                "endDate": "",
                "date": "all",
                "srchFd": "",
                "range": "",
                "author": "all",
                "authorData": "",
                "mysrchFd": ""
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=1, i",
            "referer": "https://www.koreatimes.co.kr/www2/common/search.asp",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0",
            "x-requested-with": "XMLHttpRequest"
        }
        request.cookies = {
            "ASPSESSIONIDQQDRSCQR": "KMGMGCDCDIIOIIIIKAPMJIMH",
            "hk_ltuid": "kt.1738830068692.337292.200101",
            "hkvstcd": "kt.1738830068692.337292.200101_1738830068692",
            "_clck": "1w0jwew%7C2%7Cft7%7C0%7C1863",
            "_gid": "GA1.3.309567553.1738830082",
            "_gat_gtag_UA_7749707_5": "1",
            "_gat_gtag_UA_7749707_6": "1",
            "_ga_W0W8TFL0RB": "GS1.1.1738830068.1.1.1738830091.37.0.0",
            "_ga_KBWTZDXEG5": "GS1.1.1738830068.1.1.1738830091.37.0.0",
            "keywords": "heavy%20rain",
            "_ga_EYGFPNR6QL": "GS1.1.1738830082.1.1.1738830092.50.0.0",
            "_ga": "GA1.1.1607551493.1738830068",
            "_ga_NW4GHWZDPR": "GS1.1.1738830082.1.1.1738830092.50.0.0",
            "_clsk": "yzn3i5%7C1738830092650%7C2%7C1%7Ck.clarity.ms%2Fcollect",
            "AWSALB": "r/JT4Wx3nz1CoFJv8hgbhUAQGxNRj9wFWyxVa5rF5oUv1zHVv4dFu9W//jv5TNR88Bc6j2Ay+ZmtVnKDtQWaQXXuYQqxgrqyGTD+lCSKetvEDsEo8ySYfG8t8rys",
            "AWSALBCORS": "r/JT4Wx3nz1CoFJv8hgbhUAQGxNRj9wFWyxVa5rF5oUv1zHVv4dFu9W//jv5TNR88Bc6j2Ay+ZmtVnKDtQWaQXXuYQqxgrqyGTD+lCSKetvEDsEo8ySYfG8t8rys"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//div[@class='list_article_headline LoraMedium']/a/@href").extract()
        # print(links)
        current_page = request.page

        current_links = links
        if self.previous_links is not None and current_links == self.previous_links:
            print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理
        if request.page >= 10:
            print(f"关键词 {current_keyword} 的第 {request.page} 页已经抓取完毕，退出当前关键字的循环")
            return None
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
        url = "https://www.koreatimes.co.kr/www2/common/pages/ajax_search.asp"
        params = {
            "kwd": f"{current_keyword}",
            "pageNum": f"{current_page}",
            "pageSize": "10",
            "category": "TOTAL",
            "sort": "",
            "startDate": "",
            "endDate": "",
            "date": "all",
            "srchFd": "",
            "range": "",
            "author": "all",
            "authorData": "",
            "mysrchFd": ""
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@id='startts']//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
