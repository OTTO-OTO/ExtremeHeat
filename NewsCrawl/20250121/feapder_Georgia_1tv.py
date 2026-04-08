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
        MYSQL_DB="other_country_site2",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Georgia'
    table = 'Georgia_1tv'
    keywords = ["heatwave","Extreme heatwave", "High temperature", "Extreme temperature", "Heatwave event", "Increase in high temperature", "High temperature impact", "Strong heat", "Temperature rise", "Heat event", "Temperature increase", "Heavy rainfall", "Heavy precipitation", "Torrential rain", "Extreme rainfall", "Drought", "Severe drought", "Long-term drought", "Water shortage", "Power outage", "High temperature power outage", "Heatwave power outage", "Power outage caused by high temperature", "Fire", "High temperature fire", "Heat fire", "Temperature fire", "Heat-induced fire", "Agricultural impact", "Heatwave agriculture", "Crop damage", "Agricultural heat stress", "Hypoxia", "Heatstroke", "Heat-induced heatstroke", "High temperature hypoxia", "High temperature heatstroke", "Traffic impact", "High temperature traffic", "Heatwave traffic", "Temperature traffic", "Ecological disaster", "Heat disaster", "High temperature environment", "Impact of heat on biodiversity", "Heatwave ecology", "Pollution", "High temperature pollution", "Heat pollution", "Temperature pollution", "Coral bleaching", "High temperature coral reefs", "Temperature coral bleaching" ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://1tv.ge/lang/wp-json/witv/search/"
            params = {
                "search": f"{keyword}",
                "author": "",
                "searchFrom": "",
                "searchTo": "",
                "offset": "0",
                "posts_per_page": "20",
                "lang": "en",
                "post_type": "0",
                "topics": "0",
                "filter_show": "0",
                "filter_channel": "0",
                "special_topics": "0"
            }
            yield feapder.Request(url, params=params,callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "newrelic": "eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjMzOTc5NDEiLCJhcCI6IjM1NDU4NDczNyIsImlkIjoiNTcxYmZlMTE5ODQ3NjMzYyIsInRyIjoiNWY5OWQyYTBjMGFlODliNWMzOThjY2RhYzZmZTE4ZTIiLCJ0aSI6MTczNzQ0NjQ2OTI0Nn19",
            "priority": "u=1, i",
            "referer": "https://1tv.ge/lang/en/?s=heavy+rain",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-arch": "\"x86\"",
            "sec-ch-ua-bitness": "\"64\"",
            "sec-ch-ua-full-version": "\"131.0.2903.146\"",
            "sec-ch-ua-full-version-list": "\"Microsoft Edge\";v=\"131.0.2903.146\", \"Chromium\";v=\"131.0.6778.265\", \"Not_A Brand\";v=\"24.0.0.0\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-model": "\"\"",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-ch-ua-platform-version": "\"10.0.0\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "traceparent": "00-5f99d2a0c0ae89b5c398ccdac6fe18e2-571bfe119847633c-01",
            "tracestate": "3397941@nr=0-1-3397941-354584737-571bfe119847633c----1737446469246",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "x-newrelic-id": "VwUOVl9XCRADUVVWAAIBXlQ=",
            "x-requested-with": "XMLHttpRequest"
        }
        request.cookies = {
            "cf_clearance": "4YhC_zqVQeN_jnfuyDUw5y.I3Hs2.3G2EeyFzPXEF4M-1737446353-1.2.1.1-k6Dg.UJUu4Vbl4T8Lm76ZAjS4Sg_VgvYjFNxtPlkFDfzvSgf.3TEGoga1bnoUzYe9ePdRcghWs74QfV_PLtsLrAgamm4PgttOaLBA7X_5gTS6wDRDc2Z_1aRdaL_7jF6fknVSzzyz2TWLoW91LRYH4rBIuwHWpRoLeFAE_qZ0oZgcKhVi3cClf_cEw9x4eiLMSaxjrWZnt5xZ2KEaFme_h2n_VqUXByY_2Zn6LUIP.OA.vZVTx.wgN5KqzJ_iPbVQpZgZ19sHMwtu3O0zIZwLJbCztkOA_xVe9VLXpEriZXaL1P6xtObhRLo_8Xyj5wircBcAJxKvkE6pQAjvZsZyA",
            "_fbp": "fb.1.1737446388613.56759002458845068",
            "_gid": "GA1.2.1179054750.1737446389",
            "global_nav_collapsed": "1",
            "_ga": "GA1.1.1849453314.1737446389",
            "_ga_G0CT0X9R7Z": "GS1.1.1737446388.1.1.1737446444.4.0.0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json['data'].get("posts")
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
            if "video" in item:
                continue
            items = Item()
            print(item)
            items.table_name = self.table # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url =item.get("post_permalink")
            items.title = item.get("post_title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 20
        url = "https://1tv.ge/lang/wp-json/witv/search/"
        params = {
            "search": f"{current_keyword}",
            "author": "",
            "searchFrom": "",
            "searchTo": "",
            "offset": f"{current_page}",
            "posts_per_page": "20",
            "lang": "en",
            "post_type": "0",
            "topics": "0",
            "filter_show": "0",
            "filter_channel": "0",
            "special_topics": "0"
        }
        yield feapder.Request(url, params,callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='article-intro m-t-20 post-slider-article']//p/text()").extract())
        items.author = ''
        items.pubtime = ''
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
