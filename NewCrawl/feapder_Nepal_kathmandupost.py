import json

import feapder
from feapder import Item
from lxml import etree
import re


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=2,  # 爬虫并发数，追求速度推荐32
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

    country = 'Nepal'
    table = 'Nepal'
    # 英语
    keywords = ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://cse.google.com/cse/element/v1"
            params = {
                "rsz": "filtered_cse",
                "num": "0",
                "hl": "en",
                "source": "gcsc",
                "start": "20",
                "cselibv": "5c8d58cbdc1332a7",
                "cx": "006439178574289969438:21nndnycfqd",
                "q": f"{keyword}",
                "safe": "off",
                "cse_tok": "AB-tC_5vHS_z317ZTtEzPP_Ipq3O:1740625131994",
                "sort": "",
                "exp": "cc,apo",
                "callback": "google.search.cse.api10349",
                "rurl": "https://kathmandupost.com/"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "content-length": "0",
            "origin": "https://www.namibiansun.com",
            "priority": "u=1, i",
            "referer": "https://www.namibiansun.com/search?query=heavy+rain",
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
            "device": "web",
            "browser_session_cookie": "zwuCNbyWzVnNMgKANXnDaaf7P1r73fDyDA6d1sr7x36vOySJSBHJxSvNdv7BoRoAp8WhtSP6SlIclZoTTPTD1yNx37kKLoGEJdEkBUTCbbWojB8Ths6qWEgHo5s7gVzy",
            "_ga": "GA1.1.188192741.1736320356",
            "_ga_WYRRLCT49E": "GS1.1.1736320355.1.1.1736320390.0.0.0"
        }
        return request

    def parse_url(self, request, response):
        print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        data = response.text.split("api10349(")[-1].split(");")[0]
        # print(data)
        links = json.loads(data)['results']
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
            print(item)
            items = Item()
            items.table_name = self.table
            items.article_url = item.get('url')
            items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 10
        url = "https://cse.google.com/cse/element/v1"
        params = {
            "rsz": "filtered_cse",
            "num": "10",
            "hl": "en",
            "source": "gcsc",
            "start": f"{current_page}",
            "cselibv": "5c8d58cbdc1332a7",
            "cx": "006439178574289969438:21nndnycfqd",
            "q": f"{current_keyword}",
            "safe": "off",
            "cse_tok": "AB-tC_5vHS_z317ZTtEzPP_Ipq3O:1740625131994",
            "sort": "",
            "exp": "cc,apo",
            "callback": "google.search.cse.api10349",
            "rurl": "https://kathmandupost.com/"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        # items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//section[@class='story-section']//p/text()").extract())
        items.author = ''
        items.pubtime = ''
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
