import json
import re

import feapder
from feapder import Item
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

    country = 'Hungary'
    table = 'Hungary_index'
    keywords = [
    # 'Extrém', 'Hő', 'Magas hőmérséklet',
        'Nagy mértékű eső', 'Szárazság',
    'Hő által okozott villamossági szünet', 'Tűz', 'Légszennyezés', 'Klimaatváltozás',
    'Termőterület csökkenése', 'Oxigénhiány', 'Magas hőmérséklet befolyásolja a forgalmat',
    'Ekológiai katasztrófa', 'A klímaváltozás befolyásolja a gazdaságot', 'Tengeri hőhullám',
    'Magas hőmérsékletű szennyezés', 'Korallzátony'
]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://index.hu/api/json/"
            data = {
                "datum": "2025-01-04",
                "rovat": "24ora/",
                "url_params[alllowRovatChoose]": "1",
                "url_params[ig]": "2025-01-04",
                "url_params[pepe]": "1",
                "url_params[tol]": "1999-01-01",
                "url_params[word]": "1",
                "url_params[s]": f"{keyword}",
                "url_params[p]": "1"
            }
            yield feapder.Request(url, data=data, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.stabroeknews.com/?s=heavy+rain",
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
            "cf_clearance": "MCuY3MKYk17CVss6QpYFb2MaYjnT5Xyba3TN4Tkgh7s-1735951156-1.2.1.1-6lCHvZiAPtXb8vNG55vOB34x.GguXKmyzoIPuJJDPOkGOWYiOO0nVJbMz.qHCBgmWqrpjyd8t2QHlkh62t2Mx16bYKZJP4xDNkwh7rp52yqdwqH6DjvG82Rf4Ir0L7yK.OaDmEmNWU3xq33HhYVPJU0v9zM8Oowi5mP6FxjLDbA7mW8GsEYqqo86CzSHwswYd1k915zeIA.TJNL9bLaL6eg7AnOgu9HmW9WMGLQrugiMqTKlQk.5cLUMq.bFK.P57piIUvKtvQt7gu16We0pRU2btlH5b1vTXK2M8P8TJtKYP5iVcAiX9oCJ7ayl9Hs93P2eyychs29YJErtDQpV5WQTdeCdpvO.gBDwTNrHsTWPfDs5Ke.FAwu7Yycq22l2ZMSqJ.6CZ7EeL5VcxxWvIQ",
            "_gid": "GA1.2.131689858.1735951168",
            "_gat_gtag_UA_33015976_1": "1",
            "_ga_QEZ29WX3K7": "GS1.1.1735951165.1.1.1735951184.0.0.0",
            "_ga": "GA1.2.790044143.1735951165"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json['list']
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            # print(item)
            # print(item.get("title"))
            items = Item()
            items.article_url = item.get("url")
            items.title = item.get("cim")
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://index.hu/api/json/"
        data = {
            "datum": "2025-01-04",
            "rovat": "24ora/",
            "url_params[alllowRovatChoose]": "1",
            "url_params[ig]": "2025-01-04",
            "url_params[pepe]": "1",
            "url_params[tol]": "1999-01-01",
            "url_params[word]": "1",
            "url_params[s]": f"{current_keyword}",
            "url_params[p]": f"{current_page}"
        }
        yield feapder.Request(url, data=data, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        # print(response.text)
        items = request.items
        items.table_name = self.table
        # items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='cikk-torzs']//p//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
