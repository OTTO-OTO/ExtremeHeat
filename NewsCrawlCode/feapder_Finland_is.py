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

    country = 'Finland'
    table = 'Finland_is'
    keywords =  [
    "kuuma", "ekstremi kuumaalto", "korkea lämpötila", "ekstremi lämpötila", "kuumaalto-tilanne",
    "korkea lämpötila kasvaa", "korkea lämpötilan vaikutukset", "korkea lämpötila", "voimakas kuume",
    "lämpötilan nousu", "kuuma tapahtuma", "lämpötilan nousu", "voimakas sateenkuvaus", "voimakas sateenkaatuminen",
    "tormi", "ekstremi sateenkaatuminen", "kuivaus", "severe kuivaus", "pitkäaikainen kuivaus",
    "vesihieman vaje", "sähkönkatko", "kuumaan sähkönkatko", "kuumaalto sähkönkatko", "kuumaan johtuva sähkönkatko",
    "tuli", "kuumaan tuli", "kuuma tuli", "lämpötilan tuli", "kuumaan johtuva tuli", "maatalouden vaikutukset",
    "kuumaalto maatalous", "kasvien vahingoittuminen", "maatalouden kuume stressi", "hypoxy", "kuumeen hiko",
    "kuumaan hypoxy", "kuumaan hiko", "liikenteen vaikutukset", "kuumaan liikenne", "kuumaalto liikenne",
    "lämpötilan liikenne", "ekologinen katastrofi", "kuuma katastrofi", "korkea lämpötila ympäristö",
    "kuumaan vaikutukset biodiversiteettiin", "kuumaalto ekologia", "saastuminen", "kuumaan saastuminen",
    "kuuma saastuminen", "lämpötilan saastuminen", "korallien valkeaminen", "kuumaan koralliriffit",
    "lämpötilan valkeaminen korallit"
]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = f"https://www.is.fi/api/search/{keyword}/kaikki/whenever/new/0/50/0/1768979152677/keyword/"
            yield feapder.Request(url, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "application/json",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=1, i",
            "referer": "https://www.is.fi/haku/?query=korkea&category=kaikki&period=whenever&order=new",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-model": "\"\"",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-ch-ua-platform-version": "\"10.0.0\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "_sp_su": "false",
            "euconsent-v2": "CQLlOMAQLlOMAAGABCENBXFgAP_gAEPAACQwKLwNIAFAAYABAACoAFwAMgAcAA8ACAAGQANAAfQBEAEUAJMATABOACoAFsAL4AfgBAACCAEIAJwAXIAwQBogD9AIQAREAiYBFgCOgEiAMUAaYA6gB5gECgI1ATIAqwBeYDGQGSANXAc0A-IB_IEZgI3gS4AmkBRcEw4BoACgALgAyAE4AM0AhABEQFsgMEAfEBG8CXAEwwCAkBAABYAFQAOAAeABBADIANAAiABMAD8AIQAfoBIgDFALzAZIA1cdAUAAWABUADgAIIAZABoAEQAJgAfgBOADRAH6ARaAjoCRAGKAOoAmQBVgC8wGSANXHgAQCIkIAgACwDFAHUAVYSgEAALAA4AEQAJgBigDqALzAZIUgIgALAAqABwAEAANAAiABMAD8ANEAfoBFoCOgJEAYoA6gCrAF5gMkKgAQAFFoAICrAAA.f_gAAAAAAAAA",
            "consentUUID": "f8539566-b74e-4c31-a8c4-6de26a253885_40",
            "consentDate": "2025-01-21T06:59:04.293Z",
            "sa1pid": "87f1a587-3e4d-4b0a-a623-79398a9ac67d",
            "T_ccs": "%7B%22sppd%22%3A1%2C%22spcx%22%3A1%2C%22spam%22%3A1%2C%22spma%22%3A1%2C%22spad%22%3A1%2C%22spem%22%3A1%7D",
            "_ga": "GA1.1.137215122.1737442760",
            "kndctr_A565899B544AA2E80A4C98BC_AdobeOrg_cluster": "va6",
            "kndctr_A565899B544AA2E80A4C98BC_AdobeOrg_identity": "CiY0ODQxNTI2NzU1MzkxMjYxMzE0MTcyNDkxNzU3NjIyNDAzNTEzM1ISCMbIob3IMhABGAEqA1ZBNjAAoAHOyKG9yDLwAcbIob3IMg==",
            "kndctr_A565899B544AA2E80A4C98BC_AdobeOrg_consent": "general=in",
            "_ga_BT669YV034": "GS1.1.1737442764.1.1.1737443094.0.0.474801823",
            "cp-sess": "eyJ0cmFpdHMiOiIjZjE4MSIsInNlbHMiOnsiYS1uYUN6S0didGxuIjp7ImMiOiJCIiwiZnMiOjAsInRzIjotMzcxfX0sInJ3ZHMiOnt9LCJ2biI6MSwidHZ0cyI6LTQxMiwidnRzIjoxNzM3NDQzMTMwLCJ2YWxzIjp7ImR0L3dwIjp7InYiOiJ3ZCIsInRzIjotMzZ9LCJ4LWdjcC1icS9oc19wMnBfbGlnaHRnYm0iOnsidiI6bnVsbCwidHMiOi0zNn0sIngtZ2NwLWJxL2hzX3AycCI6eyJ2IjpudWxsLCJ0cyI6LTM2fX0sIl9zdHJzIjpbIiogZ2VvL2RtOmkgdWEvb3M6dyB1YS9icjppIHVhL21vOm4gZHQvd3A6d2QiXX0%3D",
            "_ga_B8CV6HRFWC": "GS1.1.1737442759.1.1.1737443132.0.0.262145969",
            "_dd_s": "logs=0&expire=1737444052430"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json
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
            items.table_name = self.table # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url ="https://www.is.fi" + item.get("href")
            items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = item.get("displayDate")
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 50
        url = f"https://www.is.fi/api/search/{current_keyword}/kaikki/whenever/new/{current_page}/50/0/1768979152677/keyword/"
        yield feapder.Request(url, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//p[@class='article-body margin-bottom-24 padding-x-16']//text()").extract())
        items.author = ''
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
