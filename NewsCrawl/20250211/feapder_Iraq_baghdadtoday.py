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

    country = 'Iraq'
    table = 'Iraq_baghdadtoday'
    keywords = [
    "موجة حر",
    "موجات حر شديدة",
    "درجات حرارة مرتفعة",
    "درجات حرارة قصوى",
    "أحداث موجات حر",
    "زيادة درجات الحرارة",
    "تأثيرات الحرارة المرتفعة",
    "ارتفاع الحرارة",
    "حرارة قاسية",
    "ارتفاع درجات الحرارة",
    "أحداث حرارية",
    "ارتفاع معدل الحرارة",
    "أمطار غزيرة",
    "هطول أمطار شديد",
    "عواصف مطرية",
    "أمطار استثنائية",
    "جفاف",
    "جفاف حاد",
    "جفاف مطول",
    "نقص الموارد المائية",
    "انقطاع التيار الكهربائي",
    "انقطاع الكهرباء بسبب الحرارة",
    "انقطاع التيار أثناء الموجات الحارة",
    "انقطاع التيار الكهربائي بسبب الحرارة",
    "حرائق",
    "حرائق ناتجة عن الحرارة",
    "حرائق مرتبطة بارتفاع الحرارة",
    "حرائق بسبب درجات الحرارة",
    "اندلاع حرائق بسبب الحرارة",
    "تأثيرات على الزراعة",
    "موجات حر والزراعة",
    "تلف المحاصيل",
    "إجهاد حراري للمزروعات",
    "نقص الأكسجين",
    "ضربة شمس",
    "إصابة حراريّة",
    "نقص أكسجين بسبب الحرارة",
    "إجهاد حراري للبشر",
    "تأثيرات على النقل",
    "النقل في درجات الحرارة العالية",
    "المواصلات أثناء الموجات الحارة",
    "تأثير الحرارة على النقل",
    "كوارث بيئية",
    "كوارث حرارية",
    "بيئات عالية الحرارة",
    "تأثير الحرارة على التنوع البيولوجي",
    "موجات حر والنظام البيئي",
    "تلوث",
    "تلوث مصاحب لارتفاع الحرارة",
    "تلوث حراري",
    "تلوث مرتبط بدرجات الحرارة",
    "تبيض الشعب المرجانية",
    "الشعاب المرجانية وارتفاع الحرارة",
    "تبيض المرجان بسبب الحرارة"
]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://baghdadtoday.news/index.php"
            params = {
                "do": "search"
            }
            data = {
                "do": "search",
                "subaction": "search",
                "search_start": "1",
                "full_search": "0",
                "result_from": "37",
                "story": f"{keyword}"
            }
            yield feapder.Request(url, params=params, data=data, callback=self.parse_url, page=page, method='POST',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "cache-control": "max-age=0",
            "content-type": "application/x-www-form-urlencoded",
            "origin": "https://baghdadtoday.news",
            "priority": "u=0, i",
            "referer": "https://baghdadtoday.news/index.php?do=search",
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
            "_ga": "GA1.1.807224867.1739262269",
            "PHPSESSID": "5e0601683906ce1fcce097bfe162be65",
            "_ga_QP21SHVRTW": "GS1.1.1739262268.1.1.1739262437.17.0.0"
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
        links = response.xpath("//div[@class='col-lg-9']/a/@href").extract()
        print(links)

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
        url = "https://baghdadtoday.news/index.php"
        params = {
            "do": "search"
        }
        data = {
            "do": "search",
            "subaction": "search",
            "search_start": f"{current_page}",
            "full_search": "0",
            "result_from": "37",
            "story": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, data=data, callback=self.parse_url, page=current_page, method='POST',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(
            response.xpath("//div[@class='news-content']//p//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//span[@class='ms-2 time']/text()").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
