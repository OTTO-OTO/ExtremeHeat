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

    country = 'Iraq'
    table = 'Iraq_rudaw'
    keywords = [
                "heatwave", "Extreme heatwave", "High temperature", "Extreme temperature", "Heatwave event",
                "Increase in high temperature", "High temperature impact", "Strong heat", "Temperature rise",
                "Heat event", "Temperature increase", "Heavy rainfall", "Heavy precipitation", "Torrential rain",
                "Extreme rainfall", "Drought", "Severe drought", "Long-term drought", "Water shortage", "Power outage",
                "High temperature power outage", "Heatwave power outage", "Power outage caused by high temperature",
                "Fire", "High temperature fire", "Heat fire", "Temperature fire", "Heat-induced fire",
                "Agricultural impact", "Heatwave agriculture", "Crop damage", "Agricultural heat stress", "Hypoxia",
                "Heatstroke", "Heat-induced heatstroke", "High temperature hypoxia", "High temperature heatstroke",
                "Traffic impact", "High temperature traffic", "Heatwave traffic", "Temperature traffic",
                "Ecological disaster", "Heat disaster", "High temperature environment",
                "Impact of heat on biodiversity", "Heatwave ecology", "Pollution", "High temperature pollution",
                "Heat pollution", "Temperature pollution", "Coral bleaching", "High temperature coral reefs",
                "Temperature coral bleaching"
                ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.rudaw.net/API/Search/Results"
            params = {
                "Stype": "n",
                "keyword": f"{keyword}",
                "currentpage": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "devicetype": "2",
            "lang": "English",
            "priority": "u=1, i",
            "referer": "https://www.rudaw.net/english/search-results?keyword=heavy%20rain",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "x-requested-with": "XMLHttpRequest"
        }
        request.cookies = {
            "ASP.NET_SessionId": "vrb05aox4ivdks3uelwg1t0j",
            "__RequestVerificationToken": "_jPc8taQ5jPiQYRzISE-uGGTi6f6p4SmxREO8BSp30Uo_E_CMJ09fTMzkghuKqN3cyiOsMJssYP5QWmOjM7Ndn_0reo1",
            "visid_incap_619968": "wXhudFe5SkqpnmAxs1jGEItOkGcAAAAAQUIPAAAAAAAaxPjtbgx7p1DPfRJH+gYG",
            "incap_ses_1139_619968": "/KA5RgfjB309cWpfqorOD4xOkGcAAAAAIf8pjFw+NUb+vd2V3eXLqQ==",
            "_gid": "GA1.2.1282783508.1737510557",
            "_clck": "6z9tsf%7C2%7Cfss%7C0%7C1848",
            "_fbp": "fb.1.1737510557592.789962939749889138",
            "_gat": "1",
            "_ga": "GA1.1.36593655.1737510556",
            "_ga_5H757Y1N1C": "GS1.1.1737510556.1.1.1737510633.60.0.0",
            "SkwidCookie": "LastViewedPage=412386_%3fkeyword%3dheavy%2520rain&PVC-412601-0=2&UserSessionID=vrb05aox4ivdks3uelwg1t0j&PVC-5-0=1&PVC-412386-0=2",
            "_clsk": "1lzqm5k%7C1737510635621%7C4%7C0%7Ch.clarity.ms%2Fcollect"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json["Data"].get("SearchNews").get("Articles")
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
            # print(item)
            items.table_name = self.table  # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url = item
            items.title = item.get("Title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = item.get("Time")
            items.author = ''
            items.content = item.get("BodyStripped")
            print(items)
            # yield items
            # yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.rudaw.net/API/Search/Results"
        params = {
            "Stype": "n",
            "keyword": f"{current_keyword}",
            "currentpage": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='item-body']//text()").extract())

        items.author = ''
        items.pubtime = ''
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
