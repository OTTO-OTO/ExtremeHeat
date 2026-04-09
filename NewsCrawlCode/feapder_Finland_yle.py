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
        SPIDER_SLEEP_TIME=[6, 8],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB="other_country_site3",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Finland'
    table = 'Finland_yle'
    keywords = [
    "kuuma", "ekstremi kuuma aalto", "korkea lämpö", "ekstremi lämpö", "kuuma aalto -tapahtuma",
    "korkea lämpö kasvaa", "korkea lämpö vaikutus", "korkea lämpö", "vahva kuuma", "lämpö nousee",
    "kuuma tapahtuma", "lämpö nousee", "voimakas sade", "voimakas sateily", "sade", "ekstremi sade",
    "kuiva", "vakava kuiva", "pitkäaikainen kuiva", "vesi resurssien vaje", "sähkön katko",
    "korkea lämpö sähkön katko", "kuuma aalto sähkön katko", "korkea lämpö johtaa sähkön katkoon",
    "tuli", "korkea lämpö tuli", "kuuma tuli", "lämpö tuli", "korkea lämpö aiheuttaa tulen",
    "maatalous vaikutus", "kuuma aalto maatalous", "kasvi vahingoittaa", "maatalous kuuma stressi",
    "hypoxia", "kuume", "kuuma kuume", "korkea lämpö hypoxia", "korkea lämpö kuume",
    "liikenne vaikutus", "korkea lämpö liikenne", "kuuma aalto liikenne", "lämpö liikenne",
    "ekologinen katastrofi", "kuuma katastrofi", "korkea lämpö ympäristö",
    "kuuma vaikuttaa biodiversiteettiin", "kuuma aalto ekologia", "saastuminen", "korkea lämpö saastuminen",
    "kuuma saastuminen", "lämpö saastuminen", "sukeltajien valkoehto", "korkea lämpö koralliriffit",
    "lämpö valkoehto korallit"
]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://yle-fi-search.api.yle.fi/v1/search"
            params = {
                "app_id": "hakuylefi_v2_prod",
                "app_key": "4c1422b466ee676e03c4ba9866c0921f",
                "language": "fi",
                "limit": "20",
                "offset": "0",
                "query": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, method='GET',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "origin": "https://haku.yle.fi",
            "priority": "u=1, i",
            "referer": "https://haku.yle.fi/",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0"
        }
        request.cookies = {
            "userconsent": "v2|development|embedded_social_media",
            "yle_rec": "1739237170604767974428",
            "_cb": "vnmnkD9eh4aCjJjBT",
            "_chartbeat2": ".1739237194778.1739237194778.1.i1G8ZBz6wDRCV1dfmBO6sAb4-ilp.1",
            "_cb_svref": "null",
            "yle_selva": "17392371947833402485",
            "AMCVS_3D717B335952EB220A495D55%40AdobeOrg": "1",
            "AMCV_3D717B335952EB220A495D55%40AdobeOrg": "1585540135%7CMCMID%7C12158377115183670073769267773643091986%7CMCAAMLH-1739841995%7C3%7CMCAAMB-1739841995%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1739244395s%7CNONE%7CvVersion%7C4.4.0",
            "s_cc": "true",
            "_chartbeat4": "t=DBA9XUBcs3YBDbe0RLCBUotxCVu6J5&E=5&x=0&c=0.99&y=26806&w=448",
            "yleAnalyticsLink": "%7B%22position%22%3A%22%23root%20%3E%20%3Anth-child(1)%20%3E%20%3Anth-child(1)%20%3E%20%3Anth-child(3)%20%3E%20%3Anth-child(3)%20%3E%20%3Anth-child(3)%22%2C%22text%22%3A%222%22%2C%22from%22%3A%22haku.yle.fi%2F%22%2C%22to%22%3A%22haku.yle.fi%2F%22%2C%22labels%22%3A%7B%7D%2C%22mergeLabels%22%3Afalse%7D"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        # print(type(response.text))
        # print(type(response.json))
        # links = json.loads(response.text)
        # print(response.text)
        # print(response.text)
        # print(response)
        links = response.json['data']

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
            items.article_url = item.get("url").get("full")
            items.title = item.get("headline")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 20
        url = "https://yle-fi-search.api.yle.fi/v1/search"
        params = {
            "app_id": "hakuylefi_v2_prod",
            "app_key": "4c1422b466ee676e03c4ba9866c0921f",
            "language": "fi",
            "limit": "20",
            "offset": f"{current_page}",
            "query": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, method='GET',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        # items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(
            response.xpath("//section[@class='yle__article__content']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
