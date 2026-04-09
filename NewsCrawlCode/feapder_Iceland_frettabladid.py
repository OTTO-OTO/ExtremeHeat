# -*- coding: utf-8 -*-
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
        SPIDER_SLEEP_TIME=[8, 10],
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
    country = 'Iceland'
    table = 'Iceland'
    #冰岛语
    keywords = [
    "troppiska virðingin", "troppiskt lágtrykk", "troppiskur stormur", "taifún", "hurrikan", "virðing", "stormur", "mikið regn", "flóði", "stormflóði", "sælenda skemmd", "skíðblökk", "jörðskjálfta skemmd", "haf skemmd", "sterkur blæstur", "taifún skemmd", "múlaflæði", "fjallskylling"
]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.dv.is/page/1/"
            params = {
                "s": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.dv.is/?s=%C3%BEj%C3%B3f",
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
            "__ashb": "7ffa31bd9ce0072e4b635c9660add7a9d59d5d39",
            "__assc": "9fed85f72a45f98b8e0eda5999c86f996a5a6e2a",
            "_cb": "CKryQNCQm0xNDOnKvR",
            "_cb_svref": "external",
            "__utma": "63495290.1987770641.1737091256.1737091256.1737091256.1",
            "__utmc": "63495290",
            "__utmz": "63495290.1737091256.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)",
            "__utmt": "1",
            "_ga": "GA1.2.1987770641.1737091256",
            "_gid": "GA1.2.90654831.1737091256",
            "_fbp": "fb.1.1737091256664.333129222859440092",
            "_gat": "1",
            "__utmb": "63495290.2.10.1737091256",
            "_chartbeat2": ".1737091256037.1737091422228.1.MxzXNCudKCGBhi-_ChXCzPDirH8K.2",
            "__gallup": "84@eyJ1IjpbeyJ1aWQiOiJaZWs2UFZIcHBjczVsWGJJIiwidHMiOjE3MzcwOTE0MjJ9LDE3MzcxODE0MjJdfQ==",
            "_ga_RF2BGGWHM5": "GS1.2.1737091258.1.1.1737091422.0.0.0",
            "_chartbeat5": "321|498|%2F|https%3A%2F%2Fwww.dv.is%2Fsport%2F2025%2F1%2F16%2Fthetta-hafdi-thjodin-ad-segja-um-kvoldid-hvada-mikka-mus-ridli-erum-vid-eiginlega%2F|DCknnFoPlOkBeT-m4D3Ay2XDjOTMn||r|DCknnFoPlOkBeT-m4D3Ay2XDjOTMn|dv.is|::870|3070|%2Fsearch%2F%25C3%25BEj%25C3%25B3f%2F|https%3A%2F%2Fwww.dv.is%2Fpage%2F2%2F%3Fs%3D%25C3%25BEj%25C3%25B3f|CWIf9DHRI6JDw76eDBMfS8RD_gt-7||c|D6gjM0Bp3P0rTf7FB4pnrmCuuXsk|dv.is|"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//article//h1/a/@href").extract()
        # print(json.loads(links))
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
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            # items.pubtime = item.get("recs-publishtime")
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://www.dv.is/page/{current_page}/"
        params = {
            "s": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(
            response.xpath("//div[@class='textinn']//p/text()").extract()).strip()
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
