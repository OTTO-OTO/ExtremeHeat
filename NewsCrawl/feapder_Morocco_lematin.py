import json

import feapder
from NewsItems import SpiderDataItem
from lxml import etree
import re


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=1,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[5, 8],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
    )
    country = 'Montenegro'
    table = 'Montenegro_pobjeda'
    keywords = [
        'Ekstremno',
        'Vruće', 'Visoka temperatura', 'Nagli doždevak', 'Suša',
        'Prekid struje zbog vrućine', 'Pozar', 'Zračna zagađenja', 'Klimatske promene',
        'Smanjenje poljoprivrednog prinosa', 'Oksigenska nedostatak', 'Visoka temperatura utiče na saobraćaj',
        'Ekološka katastrofa', 'Klimatske promene utiču na gospodarstvo', 'Mornarska vruća vala',
        'Zagađenje visokom temperaturom', 'Koralji'
    ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://lematin.ma/search"
            params = {
                "query": f"{keyword}",
                "pgno": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://lematin.ma/search?query=Chaleur",
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
            "device": "web",
            "_gcl_au": "1.1.2063760955.1736146747",
            "_ga": "GA1.1.562754607.1736146747",
            "_cb": "u8KMrDUpYF-CCMMG0",
            "_cb_svref": "external",
            "_poool": "4ed530f7-9b88-4b0a-9ee2-e9c00fcbd831",
            "_fbp": "fb.1.1736146779412.723146579792254971",
            "_chartbeat2": ".1736146757936.1736146864354.1.BXrMfbB90KjlBgNke0fpeuiCcPtR.3",
            "_ga_BML3HY0T2H": "GS1.1.1736146746.1.1.1736146864.2.0.0",
            "PHPSESSID": "89fff850e10d0078cda9cb096dcd88d1",
            "AWSALB": "ez30jd5OUjpXAi7q2eYzkyfFCneO6vzppyDkYKZ58D9tFqv/Sk7Dj91JQgA/1jPBhOf2DEpeQ/YrnJaIJITAwXBhAVEQwuP5XRP68zVMWgHVgy16sMq43BA990Pj",
            "AWSALBCORS": "ez30jd5OUjpXAi7q2eYzkyfFCneO6vzppyDkYKZ58D9tFqv/Sk7Dj91JQgA/1jPBhOf2DEpeQ/YrnJaIJITAwXBhAVEQwuP5XRP68zVMWgHVgy16sMq43BA990Pj",
            "poool_data": "%7B%22poool_user_id%22%3A%224ed530f7-9b88-4b0a-9ee2-e9c00fcbd831%22%2C%22user_session%22%3A%2289fff850e10d0078cda9cb096dcd88d1%22%7D",
            "_chartbeat5": "622|2736|%2Fsearch|https%3A%2F%2Flematin.ma%2Fsearch%3Fquery%3DChaleur%26pgno%3D2|BlAXVsBteMYXCsKOtvjWkiOCYoA9x||c|BlAXVsBteMYXCsKOtvjWkiOCYoA9x|lematin.ma|"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//div[@class='article-info']/a/@href").extract()
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            print(item)
            items = SpiderDataItem()
            items.table_name = self.table
            items.article_url = item.get("website_url")
            items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 10
        url = "https://lematin.ma/search"
        params = {
            "query": f"{current_keyword}",
            "pgno": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@id='article-body']//p/text()").extract())
        items.author = ''
        items.pubtime = ''
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
