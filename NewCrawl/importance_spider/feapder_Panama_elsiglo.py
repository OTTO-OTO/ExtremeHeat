# 付费
import json
import re
import time
import uuid

import feapder
from feapder import Item


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[6, 10],
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
    country = 'Panama'
    table = 'Panama'
    #西班牙语
    keywords = ["Ciclón tropical", "Depresión tropical", "Tormenta tropical", "Tifón", "Huracán", "Ciclón", "Tormenta", "Lluvias intensas", "Inundación", "Marea", "Daños costeros", "Deslizamiento", "Desastre geológico", "Desastre marino", "Vientos fuertes", "Desastre de tifones", "Derrumbes de lodo", "Deslizamiento de tierra"]
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = f"https://elsiglo.com.pa/busquedas/-/search/{keyword}/false/false/19830115/20250115/date/true/true/0/0/meta/0/0/0/1"
            yield feapder.Request(url,callback=self.parse_url, page=page, keyword=keyword,filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://elsiglo.com.pa/busquedas/-/search/calor/false/false/19830115/20250115/date/true/true/0/0/meta/0/0/0/1",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "ITR_COOKIE_DEVID": "d33c223a59a13a35f599186a0828b7e052",
            "ITR_COOKIE_USRID": "d33c223a59a13a35f599186a0828b7e052",
            "ITERWEBGEO": "+RPvEJALyWbGN0msfsgPyG3ufniKoUQeusqJGW+JmPXueN4V8tjcfOSBKlfOBFWdRMstypSgJs3jCmP8d/djaQ==",
            "_pk_ses.N23PISDQNSEYQ.f277": "1",
            "_ga": "GA1.1.1939520336.1736997591",
            "_pk_id.N23PISDQNSEYQ.f277": "00d5f09d9a9f768d.1736997590.1.1736997665.1736997590.",
            "_pk_ref.N23PISDQNSEYQ.f277": "%5B%22%22%2C%22%22%2C0%2C%22%22%5D",
            "_ga_71HD91ZWES": "GS1.1.1736997590.1.1.1736997670.54.0.0"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//div[@class='headline']/a/@href").extract()
        print(links)
        # # 输出匹配的值
        # for match in matches:
        #     print(match)

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
            items.article_url =item
            # items.title = item.get("headline")
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://elsiglo.com.pa/busquedas/-/search/{current_keyword}/false/false/19830115/20250115/date/true/true/0/0/meta/0/0/0/{current_page}"
        yield feapder.Request(url, callback=self.parse_url, page=current_page, keyword=current_keyword, filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='item_template D_TXT']//p/text()").extract()).strip().replace("\r",'').replace("\n",'')
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
