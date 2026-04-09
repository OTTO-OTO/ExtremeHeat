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

    country = 'Gambia (Republic of The)'
    table = 'GambiaROZ_thepoint'
    keywords = ['heat',
                'High Temperature', 'Heavy Rain', 'Drought', 'Power Outage from Heat', 'Fire',
                'Air Pollution', 'Climate Change', 'Crop Yield Reduction', 'Oxygen Deficiency',
                'High Temperature Affecting Traffic', 'Ecological Disaster', 'Climate Change Affecting Economy',
                'Marine Heatwave', 'High Temperature Pollution', 'Coral'
                ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://cse.google.com/cse/element/v1"
            params = {
                "rsz": "filtered_cse",
                "num": "10",
                "hl": "en",
                "source": "gcsc",
                "start": "0",
                "cselibv": "5c8d58cbdc1332a7",
                "cx": "009071793635517270982:zhuk1drpfr1",
                "q": f"{keyword}",
                "safe": "off",
                "cse_tok": "AB-tC_52D8Exz07Y0pL0TGkKC53K:1740125410735",
                "sort": "",
                "exp": "cc,apo",
                "callback": "google.search.cse.api17289",
                "rurl": "https://thepoint.gm/search?q=heavy+rain"
            }
            yield feapder.Request(url, params=params,callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "referer": "https://thepoint.gm/",
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
            "__Secure-3PSIDTS": "sidts-CjIB7wV3sd8ufp4nver-MhiOP96KUtLmmcfiEtaoB-gQHYqUgSuK6hALYUPNjJnY8CrIcxAA",
            "NID": "520=UsraCm_v68gzo0iyTf32xoc9hNu1GezJCjZDhAUJDWlknHT2WVaR6Uw5wxX8fTnlqlfgPanM9MJDeOC-ux6Qwd3ZT9DJubAwlU1jDAH8_tAErLorvHwEnT9om5uYfYjZbezB6uNfed3RCR9FRHsYEhGPinOszjY8wwNcTbXe9V6X9U78CzT47kL5XzSOJ6J4VnCr41gSN5tRr-_eCTK8Iq5bJpddNRPOa2WuoDU-D52Q4AZp3O_w-JdcDTeHizKDMetypAQPu4qEaK4LmZLzGqwKq_xJ9qOG4GXGAxsDdJBrBp7_iyXRxUpA5r3M2-aPM9B_0bBVAZA4C5c3phDnInxKQhTgdGg3roP_qEr4c2mfGvVctYWaCU4l3EWNSgYuy-iY7ZZSFO-RD0chmHlAeyuu10_PCpA6VPbHG3TnTgN4ke_NSjM3hkX-TBDZr0R2iKH-rHIOdMFGbb_54Nw6YpwGE7l_81aT3eu4MQmQuH1XHNFmshsfIVA2PgDubK-rCsE0HI0iacb_BB_at3rbHdMasXSuhOo9reYbu23pNNMKqsyJLiKQSd7zfmvon8v6bUoojkFeIuKBJl9AkhqOl8-LMG6r59eojd9u6pAwlMNXbeN2xYSafHetPBXYj9AaK-y1MTjcQQcAuWC2fnVomvuxSzBFsq3JAYnyTXCHdOuZOQu8XrtiVnDgz5mh4QC6fzCF9-l_nCgMQHKxYXNexTtoE_P2GrM1uhypculDwj6xONnBn2ier7tPWtOY2uynA7aTZDIGGSRu37X-auk1UN8AYumJHe7kH1nEhIIZMw",
            "__Secure-3PSIDCC": "AKEyXzWCpFYE79-oTaqlMwyTgdetVJueIs-NQ_mYem1AGER2iO5VJIdJE62cTV1MS0eSA7_epz4"
        }
        return request


    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        data = response.text.split("api17289(")[-1].split(");")[0]
        pattern = r'"url": "(https://.*?)"'
        links = json.loads(data)['results']

        # links = re.findall(pattern, data)[0:10]
        # print("===========================>", links)
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            # print(item)
            # print(item.get("title"))
            items = Item()
            items.article_url = item.get("url")
            items.country = self.country
            items.title = item.get('title')
            items.keyword = current_keyword
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
            "cx": "009071793635517270982:zhuk1drpfr1",
            "q": f"{current_keyword}",
            "safe": "off",
            "cse_tok": "AB-tC_52D8Exz07Y0pL0TGkKC53K:1740125410735",
            "sort": "",
            "exp": "cc,apo",
            "callback": "google.search.cse.api17289",
            "rurl": "https://thepoint.gm/search?q=heavy+rain"
        }
        yield feapder.Request(url, params=params,callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        # print(response.text)
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='col-md-8']//p//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='og:updated_time']/@content").extract_first()
        print(items)
        # if items.content and "Sorry" not in items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()

