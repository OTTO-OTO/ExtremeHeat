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

    country = 'VietNam'
    table = 'Viet_Nam'
    #越南语
    keywords = ["Bão nhiệt đới", "Áp thấp nhiệt đới", "Bão nhiệt đới", "Bão", "Bão lụt", "Vòi sen", "Bão", "Mưa lớn", "Lũ lụt", "Sóng biển", "Thiệt hại ven biển", "Sạt lở", "Thảm họa địa chất", "Thảm họa biển", "Cơn gió mạnh", "Thảm họa bão", "Lở đất", "Lở đất"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://timkiem.vnexpress.net/"
            params = {
                "q": f"{keyword}",
                "media_type": "text",
                "fromdate": "0",
                "todate": "0",
                "latest": "",
                "cate_code": "",
                "search_f": "title,tag_list",
                "date_format": "all",
                "page": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page,
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://timkiem.vnexpress.net/?search_f=title,tag_list&q=n%C3%B3ng&media_type=text&fromdate=0&todate=0&latest=&cate_code=&date_format=all&",
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
            "device_env": "4",
            "sw_version": "1",
            "fosp_uid": "h9lron78yu0jzz6g.1738993358.des",
            "fosp_aid": "h9lron78yu0jzz6g.1738993358.des",
            "orig_aid": "h9lron78yu0jzz6g.1738993358.des",
            "_gid": "GA1.2.193603332.1738993359",
            "fosp_loc": "3171-0-HK",
            "_gcl_au": "1.1.774068897.1738993359",
            "_sharedID": "28f4de46-de90-4aad-a18b-6cf484a2f83c",
            "_sharedID_cst": "zix7LPQsHA%3D%3D",
            "login_system": "1",
            "fpt_uuid": "%22d9eba7e0-7be6-4cd5-900e-86e23c4b57b1%22",
            "ajs_group_id": "null",
            "cto_bundle": "2ggn3F9PT1JQUkJIcFlMNFJhcloxd0pnNGdSbFNlSmxKWm51TktTQjZuTjkwY3ZacWVuejAxSVB0V0d5OFRsWWhDNmJYTTZqSnNJNnpCY2E5QWNuSWI3aURmZ0xySmcySnJCejFyNUg2S3Y5TFpQS09EWU1rRXQ4NUQyWEtHU3Q3enFnM2tIVjRZM3c4TTNrZUE1VjhKbGU0R1ElM0QlM0Q",
            "_ga_8DHKH6QPCD": "GS1.1.1738993359.1.1.1738993496.60.0.0",
            "_ps_track_h9lron78yu0jzz6g.1738993358.des": "1",
            "_gtm_ps_track": "1",
            "_gat": "1",
            "_dc_gtm_UA-50285069-28": "1",
            "_ga": "GA1.1.1135377583.1738993359",
            "_ga_DQJ7NF9DN2": "GS1.1.1738993359.1.1.1738993566.59.0.0",
            "display_cpd": "3",
            "_ga_06PYR04YWE": "GS1.2.1738993503.1.1.1738993566.0.0.0",
            "_ga_57577CKS2C": "GS1.1.1738993359.1.1.1738993572.53.0.0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        # print(type(response.text))
        # print(type(response.json))
        # links = json.loads(response.text)
        # print(response.text)
        links = response.xpath("//article//h3/a/@href").extract()
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
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://timkiem.vnexpress.net/"
        params = {
            "q": f"{current_keyword}",
            "media_type": "text",
            "fromdate": "0",
            "todate": "0",
            "latest": "",
            "cate_code": "",
            "search_f": "title,tag_list",
            "date_format": "all",
            "page": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//p[@class='Normal']//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@itemprop='datePublished']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
