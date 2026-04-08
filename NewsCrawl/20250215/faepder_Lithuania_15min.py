# -*- coding: utf-8 -*-
"""

集群运行

"""
import re

import feapder
from feapder import Item
import json
import time
from lxml import etree


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=3,  # 爬虫并发数，追求速度推荐32
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

    country = 'Lebanon'
    table = 'Lebanon_lebanon24'
    keywords = [
    "الحرارة", "موجات الحرارة القصوى", "الحرارة العالية", "درجات الحرارة القصوى", "أحداث موجات الحرارة",
    "زيادة الحرارة العالية", "تأثير الحرارة العالية", "الحرارة العالية", "حرارة قوية", "ارتفاع درجات الحرارة",
    "أحداث الحرارة", "ارتفاع درجة الحرارة", "هطول أمطار غزيرة", "هطول أمطار قوية", "الأمطار الغزيرة",
    "هطول أمطار قصوى", "الجفاف", "الجفاف الشديد", "الجفاف طويل الأمد", "نقص موارد المياه",
    "انقطاع التيار الكهربائي", "انقطاع التيار الكهربائي بسبب الحرارة العالية", "انقطاع التيار الكهربائي بسبب موجة الحرارة",
    "انقطاع التيار الكهربائي بسبب الحرارة العالية", "الحرائق", "الحرائق بسبب الحرارة العالية", "حرائق الحرارة",
    "حرائق الحرارة", "حرائق الحرارة", "الحرائق الناجمة عن الحرارة العالية", "تأثيرات الزراعة", "زراعة موجات الحرارة",
    "إتلاف المحاصيل", "الإجهاد الحراري في الزراعة", "نقص الأكسجين", "الإغماء من الحرارة", "الإغماء من الحرارة",
    "نقص الأكسجين بسبب الحرارة العالية", "الإغماء من الحرارة العالية", "تأثيرات النقل", "النقل بسبب الحرارة العالية",
    "النقل بسبب موجة الحرارة", "النقل بسبب الحرارة", "الكارثة البيئية", "الكارثة الحرارية", "البيئة الحارة",
    "تأثير الحرارة على التنوع البيولوجي", "البيئة الحرارية", "التلوث", "التلوث بسبب الحرارة العالية", "التلوث الحراري",
    "التلوث الحراري", "البياض المرجاني", "الشُعب المرجانية الحارة", "البياض الحراري للشعاب المرجانية"
]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://cse.google.com/cse/element/v1"
            params = {
                "rsz": "filtered_cse",
                "num": "0",
                "hl": "ar",
                "source": "gcsc",
                "start": "20",
                "cselibv": "5c8d58cbdc1332a7",
                "cx": "010621872549312797348:m8nsqr05czi",
                "q": f"{keyword}",
                "safe": "off",
                "cse_tok": "AB-tC_6tmUMPL4Ml4kr7HxTekGlK:1740446617084",
                "sort": "",
                "exp": "cc,apo",
                "callback": "google.search.cse.api17566",
                "rurl": f"https://www.lebanon24.com/search/{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, method='GET',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "referer": "https://www.lebanon24.com/",
            "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Microsoft Edge\";v=\"133\", \"Chromium\";v=\"133\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "script",
            "sec-fetch-mode": "no-cors",
            "sec-fetch-site": "cross-site",
            "sec-fetch-storage-access": "active",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0"
        }
        return request

    def parse_url(self, request, response):
        print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = json.loads(response.text.split("api17566(")[-1].split(");")[0])["results"]

        print(links)
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
            items.article_url = item.get("unescapedUrl")
            # items.title = item.get("title")
            # items.pubtime = item.get("ogUpdatedTime")
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
            "hl": "ar",
            "source": "gcsc",
            "start": f"{current_page}",
            "cselibv": "5c8d58cbdc1332a7",
            "cx": "010621872549312797348:m8nsqr05czi",
            "q": f"{current_keyword}",
            "safe": "off",
            "cse_tok": "AB-tC_6tmUMPL4Ml4kr7HxTekGlK:1740446617084",
            "sort": "",
            "exp": "cc,apo",
            "callback": "google.search.cse.api17566",
            "rurl": f"https://www.lebanon24.com/search/{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, method='GET',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//meta[@property='og:title']/@content").extract_first()
        items.content = "".join(
            response.xpath("//div[@itemprop='articleBody']/div/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@itemprop='datePublished']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
