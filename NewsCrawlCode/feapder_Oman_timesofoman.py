import json

import feapder
from feapder import Item
from lxml import etree
import re


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

    country = 'Oman'
    table = 'Oman'
    # 英语
    keywords = ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://timesofoman.com/search"
            params = {
                "search": f"{keyword}",
                "page": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://timesofoman.com/search?search=heavy+rain",
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
            "_gid": "GA1.2.1705929204.1736325782",
            "_gat_gtag_UA_36328742_1": "1",
            "_gat_gtag_UA_97434832_1": "1",
            "_clck": "zazfzj%7C2%7Cfse%7C0%7C1834",
            "XSRF-TOKEN": "eyJpdiI6IlZRMFFsMHNOaTdaVXEySHV6U1Y4Wnc9PSIsInZhbHVlIjoiVFBtcEtrU3JFcVVVZHpTOVNQcnNvMTAyN3VCWmVuMEZhVlp5YkM1TGpJdUJSNXNOWkgxdWlKY3RBU3IyQ1ZRRCIsIm1hYyI6IjcyZjdkOTY2NGI5YzllNDgzMWVmN2Q3MThiM2RjMWU2ZDI4NzBkOTM4NzQ1NGQzMzI3MjQ0YjBlM2Q1ZjM2NGUifQ%3D%3D",
            "times_of_oman_session": "eyJpdiI6ImxySlwvZ2pqb0o0NlVsTER0UTBrdFwvZz09IiwidmFsdWUiOiJLcGtNSER3cHB0UkJUUmVcL3plc0tQSnROM2hLK0lPXC85dndMUEZQemZrWnRWMkE3bGJCZmc1SERtTUxsQkJYWmoiLCJtYWMiOiIyODJiMzFiMzZlNWNlZWU5YmRjZmVkMTQ0Y2M5ZDM2OGMwZWIxNzQzYTcyY2IyN2M0ODkzZmJmNTk0MTI4ODkzIn0%3D",
            "_ga": "GA1.1.1750341684.1736325781",
            "_ga_E5GLDFPF53": "GS1.1.1736325781.1.1.1736325790.0.0.0",
            "_clsk": "1u68g0r%7C1736325791989%7C3%7C1%7Cf.clarity.ms%2Fcollect",
            "_ga_GC5512BVTZ": "GS1.1.1736325780.1.1.1736325794.0.0.0"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//div[@class='row']/div//article//h2/a/@href").extract()
        print(links)
        current_page = request.page

        current_links = links
        if self.previous_links is not None and current_links == self.previous_links:
            print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理

        if request.page >= 10:
            print(f"关键词 {current_keyword} 的第 {request.page} 页已经抓取完毕，退出当前关键字的循环")
            return None
        self.previous_links = current_links  # 更新上一页的链接列表

        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            print(item)
            items = Item()
            items.table_name = self.table
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://timesofoman.com/search"
        params = {
            "search": f"{current_keyword}",
            "page": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@id='__content__']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//span[@class='text-muted']/text()[last()]").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
