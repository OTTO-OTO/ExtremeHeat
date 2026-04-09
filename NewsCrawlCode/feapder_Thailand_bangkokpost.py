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

    country = 'Thailand'
    table = 'Thailand_bangkokpost'
    # 英语
    keywords = ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]
    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://search.bangkokpost.com/search/result"
            params = {
                "start": "0",
                "q": f"{keyword}",
                "category": "all",
                "refinementFilter": "",
                "sort": "newest",
                "rows": "10",
                "publishedDate": ""
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page,
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://search.bangkokpost.com/search/result?start=10&q=heavy+rain&category=all&refinementFilter=&sort=newest&rows=10&publishedDate=",
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
            "verify": "test111",
            "_gid": "GA1.2.670444544.1738916527",
            "_fbp": "fb.1.1738916526805.674298505450082285",
            "am_FPID": "18273dc3-41bc-48d7-9e85-416ab882e8e3",
            "_clck": "1v3uvvo%7C2%7Cft8%7C0%7C1864",
            "cto_bidid": "-Tel519YVkY2QmMyT3RXT2JGNENIR2M3TG1kTmFONU9nb3JDcmNvMHp2JTJCT0xucjZSanFPM2s4WHpWWkgxdiUyQlp5V0ZvOXBBSjRnSkoyYUk1bWxmcGdhZFZjeEElM0QlM0Q",
            "cto_dna_bundle": "puVOvV9PT1JQUkJIcFlMNFJhcloxd0pnNGdSZjV4Y3BGUGg5dk5mZU01djluS0glMkJKUzQ4QmYzM2xMTm9rVk9LbEVQaHNKUXJFJTJCblVrJTJGRW5tVFYlMkZoU2VhRXhBJTNEJTNE",
            "ka_iid": "6UvTgS5Wrhd2aYT4ycjJwV",
            "ka_sid": "SG9iYUP1wE6wevAVjAhtV4",
            "_cbclose": "1",
            "_cbclose62518": "1",
            "_uid62518": "A2A7FB7A.1",
            "_ctout62518": "1",
            "_ht_v": "1738916532.4557585729",
            "_ht_s": "1738916532.2",
            "cto_bundle": "E1AWTl9PT1JQUkJIcFlMNFJhcloxd0pnNGdSVSUyRnNBRTRVWFRLWVFIJTJGSWRiV0ZZOVB0emh3RDllek94UkhJTlliRnd6TGVSQlRKaklVUEVTZTBMSDkxcWxuJTJGY2x3VW4xOE04d3VhTiUyRm9NNXBtNlpDamtEaUg4V3JtaWdzR0slMkJ1dTZPRiUyQkZQemUlMkJ3d096JTJCdW9vaGgxJTJGaTZ0JTJCdyUzRCUzRA",
            "_pubcid": "e4a115c0-2ae0-4828-9568-a9b4d4c98fcf",
            "_pubcid_cst": "zix7LPQsHA%3D%3D",
            "recentlySearched": "%5B%22heavy%20rain%22%5D",
            "_gcl_au": "1.1.484756803.1738916547",
            "_gat_UA-8091193-1": "1",
            "_ga_L5DVRVC6R4": "GS1.1.1738916526.1.1.1738916571.15.0.412175172",
            "_ga": "GA1.2.62449205.1738916526",
            "_clsk": "l3ha48%7C1738916572720%7C3%7C1%7Ck.clarity.ms%2Fcollect"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        # print(type(response.text))
        # print(type(response.json))
        # links = json.loads(response.text)
        links = response.xpath("//h3/a/@href").extract()
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
            news_id = item.split("contentId=news")[-1]
            items = Item()
            items.table_name = self.table  # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url = "https://www.bangkokpost.com/thailand/general/{}/colder-weather-moving-in-from-friday".format(
                news_id)
            # items.title = item.get("metadata").get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 10
        url = "https://search.bangkokpost.com/search/result"
        params = {
            "start": f"{current_page}",
            "q": f"{current_keyword}",
            "category": "all",
            "refinementFilter": "",
            "sort": "newest",
            "rows": "10",
            "publishedDate": ""
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='article-content']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@name='lead:published_at']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
