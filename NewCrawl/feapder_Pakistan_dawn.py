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

    country = 'Pakistan'
    table = 'Pakistan'
    # 英语
    keywords = ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://cse.google.com/cse/element/v1"
            params = {
                "rsz": "10",
                "num": "10",
                "hl": "en",
                "source": "gcsc",
                "start": "0",
                "cselibv": "5c8d58cbdc1332a7",
                "cx": "016184311056644083324:a1i8yd7zymy",
                "q": f"{keyword} inurl:/news",
                "safe": "off",
                 "cse_tok": "AB-tC_6ksWnzQn8mPu9MamXUkEgV:1740634163485",
                "sort": "",
                "exp": "cc",
                "callback": "google.search.cse.api18426",
                "rurl": f"https://www.dawn.com/search?cx=016184311056644083324%3Aa1i8yd7zymy&cof=FORID%3A10&ie=UTF-8&q={keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "referer": "https://www.dawn.com/",
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
            "NID": "520=JVJ77OHJ87Zo44y5oW8cE7-dn4wSV6v3yaQpfHu8D9S4XPQ6TzsBgbUCzIzN7GVw3-NhieQgTSzBfOqTdM76WgalPqzXyyOE5YwCQ5_waBYIA66guVHwmRoZI4D6NkKS_zOaBnRi7Vk_0Tlx6M4G6KmAN-icb_YQ0p1lv8KDeBehByeq5cMJuUYkjx0WT64jovGkaMFH3sU8bT4b1_wpwaxlBbJ8y68FpVYTMj84mwrXe2zVTTr6CXYq-ZnEQycoFXwUhiLo1m_dq9VCqD0fkCvC6tlVMzmG0FeJ2sfxo0CKY5DT7QgaLahS3QRNQVkdAe7JS6bkM7uoTnFlabi1Csmk8taiLBeA9kOfrRgx1jKvuJSTUGNppAomE2LPaz5HwvQHJqFI4SsRQ4j55Ypw5xSMai7GBRnu0VrIQ96FzA4p2puyqe7ZqOj8VrLdS6vaTawhQ7vp_P75v1njGNDd-Jo6ApdAjdkv-r5V6pmAWyh6Rc6OI749reEagUAEM06v9M6eBU-IFOIUhNmR02UmwQYmAKeIhrvw9fMTqTFd4iVDbhZCjHrP0vWvi7Y1mhocYLwev67uCfXLGuYbIwxNAheOu2J-RmdSXg7oA0CUy17z9VsvElx-aAXU6TsDHmiCIg5ZT-bNb00AUuYmPNYqMXIkcUk-Dy7LSxi4AQkir8R4ag4LtkuRGs4SzTlaRZ5iedv2l26g51scUuPWU0CHdPd7yyLvhK3mb6o-B5XwdFyK029etppMR1eKLsUGsoddzwDKP1D6SEqwcGjho1fgEe2euXsgYw2XYebRBOng0Q",
            "__Secure-3PSIDCC": "AKEyXzV8ODEjwQx6-X2cvlbUu-VoHyt042rtTQ2MaD0KZvYq8Vczw1X1upzMwBNr9X6ifUdMbkM"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        data = response.text.split("api18426(")[-1].split(");")[0]
        # print(data)
        links = json.loads(data)['results']
        # print(links)
        current_page = request.page

        current_links = links
        if self.previous_links is not None and current_links == self.previous_links:
            print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理

        self.previous_links = current_links  # 更新上一页的链接列表
        if request.page >= 10:
            print(f"关键词 {current_keyword} 的第 {request.page} 页已经抓取完毕，退出当前关键字的循环")
            return None
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
            items.pubtime = item.get("richSnippet").get("metatags").get("articleModifiedTime")
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://cse.google.com/cse/element/v1"
        params = {
            "rsz": "10",
            "num": "10",
            "hl": "en",
            "source": "gcsc",
            "start": f"{current_page * 10}",
            "cselibv": "5c8d58cbdc1332a7",
            "cx": "016184311056644083324:a1i8yd7zymy",
            "q": f"{current_keyword} inurl:/news",
            "safe": "off",
             "cse_tok": "AB-tC_6ksWnzQn8mPu9MamXUkEgV:1740634163485",
            "sort": "",
            "exp": "cc",
            "callback": "google.search.cse.api18426",
            "rurl": f"https://www.dawn.com/search?cx=016184311056644083324%3Aa1i8yd7zymy&cof=FORID%3A10&ie=UTF-8&q={current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        # items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath(
            "//div[@class='story__content  overflow-hidden    text-4  sm:text-4.5        pt-1  mt-1']//p/text()").extract())
        items.author = ''
        items.pubtime = ''
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
