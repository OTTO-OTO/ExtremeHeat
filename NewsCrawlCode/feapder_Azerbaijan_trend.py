# -*- coding: utf-8 -*-
# 173
import re

import feapder
from feapder import Item
import json
from lxml import etree


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[6, 10],
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

    country = 'Azerbaijan'
    table = 'Azerbaijan_trend'
    keywords = ["heavy rain"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://en.trend.az/search"
            params = {
                "query": f"{keyword}"
            }
            data = {
                "ajax": "true",
                "end": "0",
                # "date": "1676220720",
                "step": "1"
            }
            yield feapder.Request(url, params=params, data=data, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://en.trend.az",
            "priority": "u=1, i",
            "referer": "https://en.trend.az/search?query=heavy+rain",
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
            "cf_clearance": "LfKPKWvOpQTzrfIGnZyTIavhOExXYURMbGypcVMzCuk-1737170542-1.2.1.1-QTlbgEVdhyBIWaR08k5kO0PI8tN53Wu.oAobqk7Z68Zy.ESVVLILGRbIejPQJBLwLwM4L_j25F6.FolgYxC9gRAXggWPE8eGAl0tcw4WnSMFU.QCEu4JHpb6_D.Sv89ij5EqPlkB4ItZPi4UylR1dJRR1KvUesPijcBJScy6QbgcnBOC.vAKdTg3JuvRblDB2gNzLHyAHHYcWly3ZsZUC_4S5uM7X_W61_uHyTrc.biIXCsiCmvpkFNUeaIa6vuAZWz4zmnRqJUmIjGgJ6Z5XGaPc8pERPvuk2RMkvF26EY",
            "cookieyes-consent": "consentid:ME5aQ3ZhVjNjZEEwdElRQmVNY2xDSFU2c1VKTUl5OXQ,consent:yes,action:yes,necessary:yes,functional:yes,analytics:yes,performance:yes,advertisement:yes,other:yes",
            "_ga_3L3QD46P5N": "GS1.1.1737170570.1.0.1737170570.60.0.0",
            "_ga": "GA1.1.1565148877.1737170571",
            "G_ENABLED_IDPS": "google"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")

        links = response.xpath("//div[@class='left-column']/ul/li/a/@href").extract()

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
            items.table_name = self.table
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://en.trend.az/search"
        params = {
            "query": f"{current_keyword}"
        }
        data = {
            "ajax": "true",
            "end": "0",
            # "date": "1676220720",
            "step": f"{current_page}"
        }
        yield feapder.Request(url, params=params, data=data, callback=self.parse_url, page=current_page,
                              keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@itemprop='articleBody']//p/text()").extract())
        items.author = ''
        items.pubtime = ''
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
