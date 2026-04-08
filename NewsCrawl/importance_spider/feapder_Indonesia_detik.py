# 付费
import json
import re
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
    country = 'India'
    table = 'India'
    #印尼语
    keywords =  [
    "Ciklon tropis", "Depresi tropis", "Hurikan tropis", "Taifun", "Hurikan", "Ciklon", "Hurikan", "Hujan lebat", "Banjir", "Ombak storm", "Bencana pantai", "Goresan", "Bencana geologi", "Bencana lautan", "Angin kuat", "Bencana taifun", "Lumpur letupan", "Goresan gunung"
]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.detik.com/search/searchall"
            params = {
                "query": f"{keyword}",
                "page": "1",
                "result_type": "relevansi"
            }
            yield feapder.Request(url, params=params,callback=self.parse_url, page=page, keyword=keyword,filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.detik.com/search/searchall?query=Panas&siteid=2&source_kanal=true",
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
            "dtklucx": "gen_c5062956-3882-5981-2a0f-69a4fc361dc1",
            "___iat_ses": "B796FAA76FF9B55F",
            "___iat_vis": "B796FAA76FF9B55F.384ed2b879a455761bb6c0eca28c3085.1736994759651.e3f445195659831586054fcffb5264ce.AJOMZZOJOA.11111111.1-0.0",
            "_gcl_au": "1.1.736118831.1736994775",
            "__dtma": "146380193.1397663204.1736994775.1736994775.1736994775.1",
            "__dtmc": "146380193",
            "_gid": "GA1.2.304132123.1736994776",
            "_fbp": "fb.1.1736994776054.59093160987412869",
            "_clck": "dd8tzi%7C2%7Cfsm%7C0%7C1842",
            "_dc_gtm_UA-891770-59": "1",
            "_ga_CY42M5S751": "GS1.1.1736994777.1.1.1736994953.58.0.0",
            "last_search": "[\"Panas\"]",
            "__dtmb": "146380193.2.10.1736994954",
            "_ga": "GA1.2.1629467472.1736994776",
            "_gat_UA-891770-223": "1",
            "_clsk": "f2phyj%7C1736994955165%7C2%7C0%7Cf.clarity.ms%2Fcollect"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//h3[@class='media__title']/a/@href").extract()
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
        url = "https://www.detik.com/search/searchall"
        params = {
            "query": f"{current_keyword}",
            "page": f"{current_page}",
            "result_type": "relevansi"
        }
        yield feapder.Request(url, params=params,callback=self.parse_url, page=current_page, keyword=current_keyword, filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//article//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@name='publishdate']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
