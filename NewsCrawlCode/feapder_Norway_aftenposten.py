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
        MYSQL_DB="TropicalCyclone",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Norway'
    table = 'Norway'
    #挪威语
    keywords =["Syklon i tropene", "Tropisk depresjon", "Tropisk storm", "Tyfon", "Orkan", "Syklon", "Storm", "Kraftig regn", "Flom", "Sjøsprøyt", "Kystskade", "Skred", "Geologisk katastrofe", "Maritim katastrofe", "Kraftige vinder", "Tyfonkatastrofe", "Jordskred", "Landskred"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.aftenposten.no/sok"
            params = {
                "phrase": f"{keyword}",
                "sorting": "score",
                "page": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, method='GET',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.aftenposten.no/sok?phrase=h%C3%B8y+temperatur",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-model": "\"\"",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-ch-ua-platform-version": "\"10.0.0\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0"
        }
        request.cookies = {
            "abx2": "45",
            "abx2_shaked": "1",
            "abg": "5",
            "_sp_su": "false",
            "_cmp_marketing": "1",
            "_cmp_analytics": "1",
            "_cmp_advertising": "1",
            "_cmp_personalisation": "1",
            "consentUUID": "a0b6d9e9-40ac-40d7-b12f-7ad501e5f28f_40",
            "consentDate": "2025-01-16T03:13:49.724Z",
            "iter_id": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhaWQiOiI2Nzg4Nzk3MTQ2ZjY4Yzg2ZGY4OGIzYWIiLCJjb21wYW55X2lkIjoiNjA5NGVhMjQxNmQ1YjUwMDAxNWM2NDdlIiwiaWF0IjoxNzM2OTk3MjMzfQ.VDoaI0qGQSw-U0t_oLlYNdYmdPFIDCiUNI06HByccJ0",
            "FCNEC": "%5B%5B%22AKsRol9Ah2eeu35ZiQXbAYB5jz809kJdOhbWpjcObZmWYYcEEvaCVhX4Ckwyex8iQqiJGl7pKtpSP1-kSjCl7EqVQbrDxHkKrzfxBZqHkUxKX2PvDRzDFs90UAiApxCROUZWjlBxWpF-cBbLs7ZXp0axiaQlAWXG_Q%3D%3D%22%5D%5D",
            "VPW_Quota_301409": "KChtQktlZDQtMTE2OTg1LDE3Mzk4NTk2MTApKSxzaWc9MHhhYTMzOWNhNjgwMmMxMzQ5NDY1ZWQ3NmU0NjBkMmNmMWJmYzMzZDI2OTFmZmY2Y2RkNDE5MDAxYmU4Mjg4NjUw",
            "VPW_QuotaInfo_301409": "1|1739859610",
            "__mbl": "75@{\"u\":[{\"uid\":\"M3Jl0TaCVpgRnIjb\",\"c\":\"desktop\",\"ts\":1739254995},1739344995]}",
            "cis-jwe": "eyJpc3N1ZWRBdCI6IjIwMjUtMDEtMTZUMDM6MTM6NTJaIiwiZW5jIjoiQTEyOENCQy1IUzI1NiIsInJlSXNzdWVkQXQiOiIyMDI1LTAyLTExVDA2OjIyOjU3WiIsImFsZyI6ImRpciIsImtpZCI6IjIifQ..-ptWebnTCIs3k9PCReDHhw.xclLeZNKS1aNI1Q-MKQjrYPwNdV01VvcKsfFbZ4a3XfJhwZHkFpKUDbe-jZyxiWE5PPfKaxowqrrk-T7zUJ4DQ.7V3P91iUB0OKwLW0K3JZRw",
            "_pulse2data": "91090f71-e467-4e25-8857-dbc51d53d42c%2Cv%2C%2C1739859777000%2CeyJpc3N1ZWRBdCI6IjIwMjUtMDEtMTZUMDM6MTM6NTJaIiwiZW5jIjoiQTEyOENCQy1IUzI1NiIsInJlSXNzdWVkQXQiOiIyMDI1LTAyLTExVDA2OjIyOjU3WiIsImFsZyI6ImRpciIsImtpZCI6IjIifQ..cW3LmSAYB26mM0u0TWAQVQ.zf45PUh3qywbmxbxhWNjIZxvlFpZYiq-wrCAEzuISZlHM0SmcgTdw3Ak7oNi4Cj36DBdf-FQT4hvz4-DjSLGmA5EyVdII93k26GODsr4niSbcc1EQMDmxNW2c08n71QyDmBWkjTWnsh99oLsXPx9-QMrtUMXOd7PeeegKJ1-xaIUdWePAPMYExYcGQ9rVA43WsYV-F4UISAQux5zAf9gSQhiMM-YcKsQ_kpeoVe_JOEAeOopJaXEsh84EpClj66xS4nYJ47YIQeo5WEIvhNQASeXKVsafDj8kF7vP8pa0D1ipWCInU12WCjDgG1bqh_MLVZn0qPxi3Fth9r8gnLTHw.5TXaJB7mCL7mWPRoeSNGmg%2C%2C0%2Ctrue%2C%2CeyJraWQiOiIyIiwiYWxnIjoiSFMyNTYifQ..BdMEWqfy1qZhpBdg8ze9KcBSSPWqZIK8c1tW1HAzG6o",
            "_pulsesession": "%5B%22sdrn%3Aschibsted%3Asession%3Abc1668c3-5724-4a47-8365-a331dd93896d%22%2C1739254825455%2C1739254998186%5D",
            "_dd_s": "logs=0&expire=1739255900935"
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
        links = response.xpath("//div[@class='article-wrapper']/a/@href").extract()

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
            items.article_url = item
            # items.title = item.get("headline")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.aftenposten.no/sok"
        params = {
            "phrase": f"{current_keyword}",
            "sorting": "score",
            "page": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, method='GET',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(
            response.xpath("//article//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
