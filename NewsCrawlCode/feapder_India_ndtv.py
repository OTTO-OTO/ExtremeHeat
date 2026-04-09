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
    # 英语
    keywords =  ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = f"https://www.ndtv.com/topic-load-more/from/news/type/news/page/1/query/{keyword}"
            yield feapder.Request(url, callback=self.parse_url, page=page, keyword=keyword, filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://icelandmonitor.mbl.is/news/search/?csrfmiddlewaretoken=WwRCfNCFc4FLMT7pGq3AmjNSMnqlXR1BM75lxsOjkKzFXqOUWjhPGwVvV3OzwWpz&qs=heavy+rain&sort=0&submit=Search",
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
            "_clck": "18ugsse%7C2%7Cfsm%7C0%7C1842",
            "_clsk": "bve2zm%7C1736987604735%7C1%7C1%7Cf.clarity.ms%2Fcollect",
            "_pctx": "%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAEzIFYAODgTg4DsXAEwA2XgEYAzAAZhU4RIkAWEAF8gA",
            "_pcid": "%7B%22browserId%22%3A%22m5ylkn03a2bbsipy%22%7D",
            "__tbc": "%7Bkpex%7DFDjjktECZ2l-Y4w_aobuwdCDAYVWT9mt2JdbcUCUXdliZCgJ72jFoakAWHn8652B",
            "__pat": "0",
            "__pvi": "eyJpZCI6InYtbTV5bGtuMDhwN204Y282ZSIsImRvbWFpbiI6Ii5tYmwuaXMiLCJ0aW1lIjoxNzM2OTg3NjA3NDU2fQ%3D%3D",
            "xbc": "%7Bkpex%7DiMPQNkawXi4tMXLctTFHd9EzvUdD7ER2A7_3_BlXo08",
            "cX_P": "m5ylkn03a2bbsipy",
            "_cb": "DomRAOg2V22kW20u",
            "_chartbeat2": ".1736987610922.1736987610922.1.76gwZC6xe45ZQ8kpD7DVNsQWE75.1",
            "_cb_svref": "external",
            "_ga": "GA1.2.617732989.1736987612",
            "_gid": "GA1.2.1500737570.1736987612",
            "cX_G": "cx%3A32tbu4z68tmr1yqg03gxnfzr0%3A7djm8ukzcpby",
            "_fbp": "fb.1.1736987692776.186063568199149269",
            "_chartbeat5": "1491|26|%2Ffrettir%2F|https%3A%2F%2Fwww.mbl.is%2Ffrettir%2F|QESDVBNzwHIDO8QbhmHjUwDO4p8||c|CLIHu8CufJN4B8JXN_duJNzBDyEMJ|mbl.is|",
            "_chartbeat4": "t=YDqzYCORIuFBqfTeFpjVf5D3PMs2&E=41&x=0&c=2.98&y=20687&w=418",
            "__gallup": "84@eyJ1IjpbeyJ1aWQiOiJuZWVRMVRucm9nRG54N0o4IiwidHMiOjE3MzY5ODc4MDl9LDE3MzcwNzc4MDldfQ=="
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//h3[@class='SrchLstPg_ttl-lnk']/a/@href").extract()
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
            items.article_url = item
            # items.title = item.get("headline")
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://www.ndtv.com/topic-load-more/from/news/type/news/page/{current_page}/query/{current_keyword}"
        yield feapder.Request(url, callback=self.parse_url, page=current_page, keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@id='ignorediv']//p/text() | //div[@itemprop='articleBody']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@name='publish-date']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
