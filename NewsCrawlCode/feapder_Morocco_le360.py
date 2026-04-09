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

    country = 'Morocco'
    table = 'Morocco'
    # 法语
    keywords = ["Cyclone tropical", "Dépression tropicale", "Tempête tropicale", "Typhon", "Ouragan", "Cyclone", "Tempête", "Pluies diluviennes", "Inondation", "Houle", "Dommages côtiers", "Glissement", "Catastrophe géologique", "Catastrophe marine", "Vents violents", "Catastrophe de typhon", "Glissement de terrain", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://36e79gum0z-3.algolianet.com/1/indexes/*/queries"
            params = {
                "x-algolia-agent": "Algolia for JavaScript (4.14.2); Browser; instantsearch.js (4.67.0); react (18.2.0); react-instantsearch (7.7.2); react-instantsearch-core (7.7.2); JS Helper (3.18.0)"
            }
            data = {
                "requests": [
                    {
                        "indexName": "le360-le-360-francais-search",
                        "params": f"facets=%5B%22section%22%2C%22subtype%22%5D&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=10&maxValuesPerFacet=10&page=1&query={keyword}&tagFilters="
                    }
                ]
            }
            data = json.dumps(data, separators=(',', ':'))
            yield feapder.Request(url, params=params, data=data, callback=self.parse_url, page=page, keyword=keyword,
                                  method='POST',
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Origin": "https://fr.le360.ma",
            "Referer": "https://fr.le360.ma/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "cross-site",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0",
            "content-type": "application/x-www-form-urlencoded",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "x-algolia-api-key": "a5a488219b95319eac9925ccbfdb4618",
            "x-algolia-application-id": "36E79GUM0Z"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json["results"][0].get("hits")

        # print(links)
        current_page = request.page

        current_links = links
        if self.previous_links is not None and current_links == self.previous_links:
            print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理

        self.previous_links = current_links  # 更新上一页的链接列表
        if request.page >= 10:
            print(f"关键词 {current_keyword} 的第 {request.page} 页已经抓取完毕，退出当前关键字的循环")
            return None
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            items = Item()
            items.table_name = self.table  # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url = "https://fr.le360.ma" + item.get("websiteUrl")
            items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://36e79gum0z-3.algolianet.com/1/indexes/*/queries"
        params = {
            "x-algolia-agent": "Algolia for JavaScript (4.14.2); Browser; instantsearch.js (4.67.0); react (18.2.0); react-instantsearch (7.7.2); react-instantsearch-core (7.7.2); JS Helper (3.18.0)"
        }
        data = {
            "requests": [
                {
                    "indexName": "le360-le-360-francais-search",
                    "params": f"facets=%5B%22section%22%2C%22subtype%22%5D&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=10&maxValuesPerFacet=10&page={current_page}&query={current_keyword}&tagFilters="
                }
            ]
        }
        data = json.dumps(data, separators=(',', ':'))
        yield feapder.Request(url, params=params, data=data, callback=self.parse_url, page=current_page,
                              keyword=current_keyword, method="POST",
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(
            response.xpath("//article//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//div[@class='subheadline-date']/text()").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
