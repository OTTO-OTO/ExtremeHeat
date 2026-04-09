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
        MYSQL_DB="other_country_site2",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Jamaica'
    table = 'Jamaica_jamaica_gleaner'
    keywords =["heatwave","Extreme heatwave", "High temperature", "Extreme temperature", "Heatwave event", "Increase in high temperature", "High temperature impact", "Strong heat", "Temperature rise", "Heat event", "Temperature increase", "Heavy rainfall", "Heavy precipitation", "Torrential rain", "Extreme rainfall", "Drought", "Severe drought", "Long-term drought", "Water shortage", "Power outage", "High temperature power outage", "Heatwave power outage", "Power outage caused by high temperature", "Fire", "High temperature fire", "Heat fire", "Temperature fire", "Heat-induced fire", "Agricultural impact", "Heatwave agriculture", "Crop damage", "Agricultural heat stress", "Hypoxia", "Heatstroke", "Heat-induced heatstroke", "High temperature hypoxia", "High temperature heatstroke", "Traffic impact", "High temperature traffic", "Heatwave traffic", "Temperature traffic", "Ecological disaster", "Heat disaster", "High temperature environment", "Impact of heat on biodiversity", "Heatwave ecology", "Pollution", "High temperature pollution", "Heat pollution", "Temperature pollution", "Coral bleaching", "High temperature coral reefs", "Temperature coral bleaching" ]

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
                "cx": "partner-pub-4993191856924332:98b6e2-dgz1",
                "q": f"{keyword}",
                "safe": "active",
                "cse_tok": "AB-tC_5NrbZjZsXz4KgICtk-W_iC:1740376775398",
                "sort": "",
                "exp": "cc,apo",
                "callback": "google.search.cse.api8039",
                "rurl": "https://jamaica-gleaner.com/search?cx=partner-pub-4993191856924332%3A98b6e2-dgz1&cof=FORID%3A10&ie=ISO-8859-1&q_as=heavy+rain&sa=Search&siteurl=jamaica-gleaner.com%2F&ref=jamaica-gleaner.com&ss="
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "referer": "https://jamaica-gleaner.com/",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "script",
            "sec-fetch-mode": "no-cors",
            "sec-fetch-site": "cross-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "__Secure-3PSID": "g.a000rggyq3rN6l5a4RzrqfKR8L6w_4oSKSt2-NpZPRMnn-bzQbIYlvrEO6wyUoA2X7nR4dKaLQACgYKAc8SARcSFQHGX2Mi1LRifzVo7z1Yh0GO1dLi3BoVAUF8yKq57cjKXq68AYkR7eUXDFJW0076",
            "__Secure-3PAPISID": "IMadT4_VMCUqIjYe/AiQnZJDT7r7FsfPRc",
            "__Secure-3PSIDTS": "sidts-CjIBmiPuTVoGkIfi9mvTUVCKD4hmvWP9DC0RsXoi3RWh47P6hfQWJqQPKo0a3SsC9ZUW7RAA",
            "NID": "520=kPN-7Mb_OK0AEfbFlDLc02aHXea0x-V0OsfWrZD9RX2kKDWVabbFslTyAeD5e-ztET1Sct2mQmdvZ9Fssq5ug59qvfDkTSjrOWpKJwPokUwXKr7TLHCg0WS8zdWRKbiVloqpqoHv5xu9BwHWSuzkFA0_G1KpLF0ds8txqYFveffhA1Z9lePVZo6DDgxfcxJNzdtCcwd6CzPHdoPZfI-E24-LueTJZIRfu-DeJstM7NvA8OAIvB9b2BrvQ3Gcys1aMVJdz3qqWt4q5qadrsKHKHo7fKNMF7gwSAoDO3JQvcjVwesS90W3nlMjJOtgPZUC3n2pmJx5I7t4guwhyfrIfx6exerm-edDXoNq--I2hiUNdZmBthbklrxf2Is7cYyGXlgFkpqIv1gdPo_AzeEEOgz0pZEGd0tnXRa3vJ-q5aqzBvJyoLVwRHFZulZhTBfa2kFdEa7eoh6RIaV-6Jteq5pkQIb01I3Fx-gBcAa_Q6cC2ArRJO5cPjD9qB0TPFamxbJ3uikgH5B2b1hQvFt-p6L_vBVOTNIcBGmcN7Cz8kKV8sogvSN24NRxoPHpEqHf8XRa7A6IjsprGO96ssEhq86xedg1GRhqAkonzS3W7DRxFETLDBxXaHl2TBsuapccfRBG60ziXTRlHHxhXGc1otaUQVgDhh3aiLgqY1_B9qhqrwO1sgpgNDtxtjw4u-_Ymt_4YrgPFiIEjO8kTuhaP5HYDwjDeVaQilxKzRCElgONTeCzgH6UeLcgpfVFKCUJtWpAxSLYmDWWGpdvupycrFTfKyeiWf38kQlwUUV9o6n6BizMSMht8Ov0IvSWkeRAny51dbiFO0dlfffp",
            "__Secure-3PSIDCC": "AKEyXzW5DX66JDlCtozr3hmYS_W3Zr6HUfF38zc654kF90CqSMKZDLx8UWDJJaYDjKZiw_IbTAk"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = json.loads(response.text.split("api8039(")[-1].split(");")[0])["results"]

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
            items.article_url =item.get("url")
            items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 10
        url = "https://cse.google.com/cse/element/v1"
        params = {
            "rsz": "filtered_cse",
            "num": "10",
            "hl": "en",
            "source": "gcsc",
            "start": f"{current_page}",
            "cselibv": "5c8d58cbdc1332a7",
            "cx": "partner-pub-4993191856924332:98b6e2-dgz1",
            "q": f"{current_keyword}",
            "safe": "active",
            "cse_tok": "AB-tC_5NrbZjZsXz4KgICtk-W_iC:1740376775398",
            "sort": "",
            "exp": "cc,apo",
            "callback": "google.search.cse.api8039",
            "rurl": "https://jamaica-gleaner.com/search?cx=partner-pub-4993191856924332%3A98b6e2-dgz1&cof=FORID%3A10&ie=ISO-8859-1&q_as=heavy+rain&sa=Search&siteurl=jamaica-gleaner.com%2F&ref=jamaica-gleaner.com&ss="
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        # items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='article-content ']//p/text()").extract())

        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
