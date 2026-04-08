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
    country = 'Venezuela'
    table = 'Venezuela'
    #西班牙语
    keywords = [
        "calor"
    ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.eluniversal.com/buscador"
            data = {
                "query": f"{keyword}",
                "pagina": "1",
                "seccioncod": ""
            }
            yield feapder.Request(url, data=data, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "cache-control": "max-age=0",
            "content-type": "application/x-www-form-urlencoded",
            "origin": "https://www.eluniversal.com",
            "priority": "u=0, i",
            "referer": "https://www.eluniversal.com/buscador",
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
            "_pubcid": "2f7b702b-b940-4528-b466-5bfd5ffee609",
            "_pubcid_cst": "zix7LPQsHA%3D%3D",
            "pbjs-unifiedid": "%7B%22TDID%22%3A%22eaf74213-57c5-413d-bf6b-80abaee1200e%22%2C%22TDID_LOOKUP%22%3A%22FALSE%22%2C%22TDID_CREATED_AT%22%3A%222025-01-18T01%3A29%3A17%22%7D",
            "pbjs-unifiedid_cst": "zix7LPQsHA%3D%3D",
            "_gid": "GA1.2.731276099.1737163773",
            "panoramaId_expiry": "1737768558003",
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "panoramaId": "c2ba4404dd1662779c299379ee5816d53938f08b6896985ba839d86735aa250d",
            "sharedid": "fd2e76ba-110c-4a78-947f-4cdfc90559b8",
            "sharedid_cst": "zix7LPQsHA%3D%3D",
            "cnx_userId": "74c863f4609844a58219d74ba95f5e93",
            "__gads": "ID=1f678d85d0e1944c:T=1737163787:RT=1737163787:S=ALNI_MYl6EByQPvIpH72eUHVcs5Q51q8JQ",
            "__gpi": "UID=00000fef45aca82a:T=1737163787:RT=1737163787:S=ALNI_MbmU9cHM0VVq35mj-sk7y1x5mHKcg",
            "__eoi": "ID=41b1f67e42cb3919:T=1737163787:RT=1737163787:S=AA-AfjZpIxIwd6XLonIf1JT3s0P_",
            "_ga_8TWDKV9V2E": "GS1.2.1737163773.1.1.1737163975.0.0.0",
            "_ga": "GA1.1.1030938486.1737163773",
            "cto_bundle": "Uy98N19PT1JQUkJIcFlMNFJhcloxd0pnNGdYNnhRaiUyQkwlMkZaZDVWcSUyRjZ2YzFSQWJpZThOJTJCOEFwSzBPZlAwM0hFTGtKSGllRGVpOHoxTFVuUzlIRlhqdzhZenVRanRRUXdramt1SGFxQmlGeFBYVjhCbzZ5a2VYM3MwJTJGMm5GU3l4dFV4OWN1M0ZzbTM1YXdMVGwyRFd3blJvUyUyQnclM0QlM0Q",
            "cto_bidid": "Qbf9Y19qY0ZEc0NHN2hXWmpkRkQ1TCUyRmlCaCUyQkZhUVU3ZXZVam1uRCUyRnlTelNzdk5xajVZSllzajMxcjF0azhHVWZHZDB5bGlOQnBCQm56VkM1JTJGYXJxd2FRUmRyM2FBbm5zZjE1bktxOUpFWkxLTDE0cW5oMiUyRkEzV2Fpb1c1cXNmbDJmamU",
            "cto_dna_bundle": "XWZ6mV9PT1JQUkJIcFlMNFJhcloxd0pnNGdSZjV4Y3BGUGg5dk5mZU01djluS0glMkJKUzQ4QmYzM2xMTm9rVk9LbEVQaHNJMnhHdEJUNXZJNnpJU0xiZTh6ZHVRJTNEJTNE",
            "_ga_EKR7DSLH6Q": "GS1.1.1737163797.1.1.1737164010.0.0.0"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//div[@class='col-xs-4 paddingtop']/div/a/@href").extract()
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
        url = "https://www.eluniversal.com/buscador"
        data = {
            "query": f"{current_keyword}",
            "pagina": f"{current_page}",
            "seccioncod": ""
        }
        yield feapder.Request(url, data=data, callback=self.parse_url, page=current_page, keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='body-note-b']//div/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
