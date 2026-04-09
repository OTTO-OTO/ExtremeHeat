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

    country = 'Philippines'
    table = 'Philippines'
    #英语
    keywords = ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]
    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://cse.google.com/cse/element/v1"
            params = {
                "rsz": "filtered_cse",
                "num": "10",
                "hl": "zh-CN",
                "source": "gcsc",
                "start": "0",
                "cselibv": "8fa85d58e016b414",
                "cx": "23ec5cf979a8e4812",
                "q": f"{keyword}",
                "safe": "off",
                "cse_tok": "AB-tC_4yPUdFJTRs3xa3lX5L800G:1740636158279",
                "lr": "",
                "cr": "",
                "gl": "",
                "filter": "0",
                "sort": "",
                "as_oq": "",
                "as_sitesearch": "",
                "exp": "cc,apo",
                "callback": "google.search.cse.api8541",
                "rurl": f"https://www.abs-cbn.com/search?q={keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "referer": "https://www.abs-cbn.com/",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "script",
            "sec-fetch-mode": "no-cors",
            "sec-fetch-site": "cross-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "__Secure-3PSID": "g.a000rggyq3rN6l5a4RzrqfKR8L6w_4oSKSt2-NpZPRMnn-bzQbIYlvrEO6wyUoA2X7nR4dKaLQACgYKAc8SARcSFQHGX2Mi1LRifzVo7z1Yh0GO1dLi3BoVAUF8yKq57cjKXq68AYkR7eUXDFJW0076",
            "__Secure-3PAPISID": "IMadT4_VMCUqIjYe/AiQnZJDT7r7FsfPRc",
            "__Secure-3PSIDTS": "sidts-CjIB7wV3sZ_WCRgS-4Eo-OfDw1LVWW6xLN412SrHzKTY_1NtCcWRsa4lQ__XCEtox88CqxAA",
            "NID": "520=XXVByk80kIHLBf5MZzyb-R0PVPjKb3bKOd0Xen0_P5RrLlgjm8tIf07LvmOia2nMRvUlrOowSJ4B20iaqutuTrkI64knMA8HUapSKk-nnCvQ0YYPnxUInjP6OyuOD03-SG46irdOPUtiYV-n4lqnXBWHTfND2mAn63ZVPMU1tZ_BrOfKxXV1CiKNemzpbCRR9OfY2V3KrCgzK3qsV_0TZaKgs_B_RpuA6Zke6hjjs9o72eIJ6WlhAxd4SEWvsVFrEYT6gIDLBMVsdRE7P5XdF0LUG5SkE8OXJExrx-Cx3LqExB1Fn-fw4qW6XiwZrX-70afjD4bNfJbk9dwPo8PdoHSCvfOrR7gJ_Tv0JCGVoAQK7ojJAwiOjV3cgnIwuBMnCmfUEXglA0QQiLGXXQgy4fmLjObq5luyabn_vKUB0izQ9858LEm_CJz4Kpl5wWsdy8qutlPbEbiHs7aedBUlK4xepABKR04hmYjBkYaZma7bOEk6YOlYrRrDp828JWHmxEJYsXw4tR8rctSDxTjAvQZFpQEY216UTyOFw66xaeMVmvDcrMr7AijloW51D3bVzroZg4M78u7rw09s4rR2FgnCOSHCe8CNTUNix4qKdllmRRWLJ1pzUZxR4WeLO9fbCdf3lHS6-kpwPB7sWymfhhO1TsrupmxRjcRHCFr5aGQLP27nDL60EOlHWkA-RGDZAYlsWLZa6qzE_mNVYY1WwBbnODo3rMkfVw07pVdKnvmgeqgEpA3RHBOPHoHp7t8GQKjKEYv6sWAugmW7ejCTcq9irFvF6ACMUdB5GwScmw",
            "__Secure-3PSIDCC": "AKEyXzWa4n9FM4gYiUCzFBrD-yz1G0ZTYy5i-HCdccOb1g7ceU-Esso2cWO_U7i5l_NAsB1_Od8"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        data = response.text.split("api8541(")[-1].split(");")[0]
        links = json.loads(data)["results"]
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
            items.article_url = item.get("url")
            items.title = item.get("title")
            items.pubtime = item.get("richSnippet").get("metatags").get("articleModifiedTime")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://cse.google.com/cse/element/v1"
        params = {
            "rsz": "filtered_cse",
            "num": "10",
            "hl": "zh-CN",
            "source": "gcsc",
            "start": f"{current_page * 10}",
            "cselibv": "8fa85d58e016b414",
            "cx": "23ec5cf979a8e4812",
            "q": f"{current_keyword}",
            "safe": "off",
            "cse_tok": "AB-tC_4yPUdFJTRs3xa3lX5L800G:1740636158279",
            "lr": "",
            "cr": "",
            "gl": "",
            "filter": "0",
            "sort": "",
            "as_oq": "",
            "as_sitesearch": "",
            "exp": "cc,apo",
            "callback": "google.search.cse.api8541",
            "rurl": f"https://www.abs-cbn.com/search?q={current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        # items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@id='bodyTopPart']//div//p/text()").extract())
        items.author = ''
        items.pubtime = ''
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
