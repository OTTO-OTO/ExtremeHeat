
import json
import re

import feapder
from NewsItems import SpiderDataItem
from curl_cffi import requests
from lxml import etree


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[8, 10],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB="other_country_site1",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )

    country = 'Haiti'
    table = 'Haiti_haitilibre'
    keywords = ['climate']
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.haitilibre.com/cat-14-environnement-1.html"
            yield feapder.Request(url, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.haitilibre.com/cat-14-environnement-86.html",
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
            "PHPSESSID": "2qsg38mpsjj425gf8bufo9ius6",
            "_ga": "GA1.1.2101097931.1735952041",
            "__gads": "ID=24ac978924c3baa8:T=1735952035:RT=1735952035:S=ALNI_Maar4bhmYQ5k9nG1A9yZv6svXH2Lw",
            "__gpi": "UID=00000fd2f0548d57:T=1735952035:RT=1735952035:S=ALNI_MZ4wNq8mq5rFSspuQIDK4bNXB17cA",
            "__eoi": "ID=e8344394cfdddad5:T=1735952035:RT=1735952035:S=AA-AfjYvEJDMB2lTfnNmWbrCX5Iy",
            "_ga_K1P6PDYBZX": "GS1.1.1735952041.1.1.1735952239.57.0.0",
            "FCNEC": "%5B%5B%22AKsRol86Jnle5UHkhhKMH2eFIK71u06fd1d_WcS18hAFpqmjIcrP2tqK4QC5w9QKjUQ4lE76DA0AACfR7pUtxaNYz4vDzyE6Ai_CzzVIUjvbfld6z1QY72C8zQFq8JYdfKPeI3JWjMj3wuZfgGwiE5iEL3OTUuKyfQ%3D%3D%22%5D%5D"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//td[@class='text']/a/@href").extract()
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            print(item)
            # print(item.get("title"))
            items = SpiderDataItem()
            items.article_url = item
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://www.haitilibre.com/cat-14-environnement-{current_page}.html"

        yield feapder.Request(url, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        # print(response.text)
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("(//td[@valign='top'])[4]/text()").extract()).replace("\n", "")
        items.author = ''
        items.pubtime = response.xpath("//span[@class='date']/text()").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
