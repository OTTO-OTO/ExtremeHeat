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

        SPIDER_THREAD_COUNT=3,  # 爬虫并发数，追求速度推荐32
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

    country = 'Philippines'
    table = 'Philippines'
    # 英语
    keywords = ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://cse.google.com/cse/element/v1"
            params = {
                "rsz": "filtered_cse",
                "num": "10",
                "hl": "en",
                "source": "gcsc",
                "start": "0",
                "cselibv": "5c8d58cbdc1332a7",
                "cx": "partner-pub-3470805887229135:3785249262",
                "q": f"{keyword}",
                "safe": "active",
                 "cse_tok": "AB-tC_6gjhSlPDUniwiXdtpQWmr6:1740636581341",
                "sort": "",
                "exp": "cc",
                "callback": "google.search.cse.api7004",
                "rurl": f"https://www.inquirer.net/search/?q={keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "referer": "https://www.inquirer.net/search/?q=heavy+rain",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "script",
            "sec-fetch-mode": "no-cors",
            "sec-fetch-site": "cross-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0"
        }
        request.cookies = {
            "__Secure-3PSID": "g.a000rggyq3rN6l5a4RzrqfKR8L6w_4oSKSt2-NpZPRMnn-bzQbIYlvrEO6wyUoA2X7nR4dKaLQACgYKAc8SARcSFQHGX2Mi1LRifzVo7z1Yh0GO1dLi3BoVAUF8yKq57cjKXq68AYkR7eUXDFJW0076",
            "__Secure-3PAPISID": "IMadT4_VMCUqIjYe/AiQnZJDT7r7FsfPRc",
            "__Secure-3PSIDTS": "sidts-CjIBmiPuTVoGkIfi9mvTUVCKD4hmvWP9DC0RsXoi3RWh47P6hfQWJqQPKo0a3SsC9ZUW7RAA",
            "NID": "521=rboE0EifOCKdA8Wlm_wYJ1R8pwzLIDDfgXDL6cnd7e8GzCbAZa_7DYJTHayJvNS-kW2DjmoFkVe1MiItAS6F0-0vGPYMbxeaVmVhpP81K6n-zmnwR0-FtjS7Z75uZya2a82J4kyEjlLYn_Tr-IBZNZQuL_FqmMTaIu5FJcVGTxDNQrmhO0J_844NjHSPez8CG3JZzNyTjY2QAcjOtCUyQ1UWs7dwEKQECMdxX7hA7qfeedBhZM-y6l7l9wLJOK6zUnyTc74gUH2Tp4ZHG8UkgWbLyNpl5lLe9fsOW2CQOCPjtcrpTCapu8DXPALCmMODxAvY_85vBaMlslh8JJnDEBC4o0t6W1z9uv24yrdn313M2TJ5QKEtpv2Ossd-qKXA0d8K4Vii3s-HEcwmqZWHPeUD7qpX093Br0rAa1veak3CGQQFYGD4RUO8FvbvIIe4Gby1dyTW5ds5z31bo_l5OOeKYE5zzlaylYf1mztQBAJDKshCjIlz6m6RZZ8vU5I0qTqvbxvtLE2S7eNWELz5-OUgmoM7Wjo0k90jH5AA5wqOn2wAAwuz5vIGdzZU4hvQhWVdaNBiQ1DtLV8GF4XhYzPt3BMCL0FXNxWiKS9I9NfhzcL2l0kEds_K4r3nDZWbneUdu9VWJn0Yeoj1K3XYuKCt0mIiOW_5QNNOLumjh6IKi7wx7b1xfsebxGZ66jZu4zdIh-d3ZW9SYR-yb13BT5qY7w6KZz8O0aq1RIqL8FCjcjLWeEWhNsSE7mOmPzFKIGtt0vXH0D4zKBOJXD4_vy-oLwAneAfqFyXHkuEA9d1maKTA-f_sahQYBqH961y6rNJvk4cR4eKh5qMe",
            "__Secure-3PSIDCC": "AKEyXzUqm8fb8o065qfLzbj-pwr9cHgm6A0z_WdBUsQp8YY1pkPzBmzgsimt0q7qmrUMqeDus9g"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = json.loads(response.text.split("api7004(")[-1].split(");")[0])['results']

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
            print(item)
            items.table_name = self.table  # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url = item.get("url")
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = item.get("richSnippet").get("metatags").get("articleModifiedTime")
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://cse.google.com/cse/element/v1"
        params = {
            "rsz": "filtered_cse",
            "num": "10",
            "hl": "en",
            "source": "gcsc",
            "start": f"{current_page * 10}",
            "cselibv": "5c8d58cbdc1332a7",
            "cx": "partner-pub-3470805887229135:3785249262",
            "q": f"{current_keyword}",
            "safe": "active",
             "cse_tok": "AB-tC_6gjhSlPDUniwiXdtpQWmr6:1740636581341",
            "sort": "",
            "exp": "cc",
            "callback": "google.search.cse.api7004",
            "rurl": f"https://www.inquirer.net/search/?q={current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@id='article_content']//p/text()").extract())
        items.author = ''
        items.pubtime = ''
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
