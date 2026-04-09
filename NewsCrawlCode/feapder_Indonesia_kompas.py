import re

import feapder
from feapder import Item
from curl_cffi import requests
from lxml import etree


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=3,  # 爬虫并发数，追求速度推荐32
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

    country = 'Indonesia'
    table = 'Indonesia'
    # 英语
    keywords =  ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]

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
                "cx": "partner-pub-9012468469771973:uc7pie-r3ad",
                "q": f"{keyword}",
                "safe": "active",
                "cse_tok": "AB-tC_4Ze_YlP_wKRVixeMkkrmtK:1740990064789",
                "lr": "",
                "cr": "",
                "gl": "",
                "filter": "0",
                "sort": "",
                "as_oq": "",
                "as_sitesearch": "",
                "exp": "cc",
                "callback": "google.search.cse.api3877",
                "rurl": "https://search.kompas.com/search/?q=Heavy+rain&submit=%E6%8F%90%E4%BA%A4"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "referer": "https://search.kompas.com/",
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
            "__Secure-3PSIDTS": "sidts-CjIB7wV3sWROYgMyzEY_g6Mxo7yEot0qU8nqt2iJULIFF6txaxVZokkiYc3qBur4mIjj8xAA",
            "NID": "520=NBLfaJwW-HcQx_Kiu5n3viDBslCiKhQKhsrhmrFbkRYuTwXf5HYvHkUeMt0QX_8gTfGdiKgz_nvQX_fN-7KoKShb35eEYtrMasH1PGyuARGqgNojF_zOvOHu-T7JOkiETweBJzb56uA5LLKeTnaSv99XjlowX2yKMHk7_dcC4srSxv0UiQ7Hiaxkoh7sQb9ixd1fc09Q9MeGE1mFU8gVXu4D5xTwDHdK_dTjuf_b-5UZUDvSL2AZ1Y8_pUlldVrDSQyyC-K566z2AoHLLw0yrET8xHp-FPRIyXSeO4YBviI_ENHeG120vfTI2zflJsBr-zBeXPZx1BPTFmoOYKEe0RodRSojc0BL3axk_xNmZGWhuUbgGBxfFpyU3RpIL8kgGcIhM_lhxkW2TKiRnrJdjd9oLj5kyrCmcMdQEwPmNfvbK5l2DKaL9FRm7uersGDSiyb489GrC4mvN1fliVHNXtkaeS7XDwMeu1phRUwvdAI_kC5sL8_EGZb5vtE9xg2h2cVi-Gu1XI9emmxyTkCt-W0IKV2ahRN7U0IA9_PecglRX4D-QJWjH6W0MzenI9L9lIE6wiUG5pWKfg99orXsUnCPpY7LgEdsCaf-Z_8Ea0hMy5Wl1cBhaiuEHIAJoJ2cXTDqBKFrZsXzpbO8JEnu97VEiuUHq6ikcrCj3N4DILsCQwP-47yKezlIx1GevTN-sfJtbgnFfj7scoAtqP1xYwg8P3fYvtrAe_pHogdiPxcd5wI_-q0qtlOpD6a85yz4VEVCKU8iuA_LdQiwCdKtpuoBsJTB8sgIabPfW25uAQ",
            "__Secure-3PSIDCC": "AKEyXzXCC_I8A9XzPm0u4nxxN5IteolRLQsemk9TZKFzy01pBPB4E_qcp0Od-X_Lo255PfFQTF4"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        data = response.text.split("api3877(")[-1].split(");")[0]
        pattern = r'"url": "(.*?)"'
        links = re.findall(pattern, data)
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links[:10]:
            print(item)
            items = Item()
            items.article_url = item
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
            "cx": "partner-pub-9012468469771973:uc7pie-r3ad",
            "q": f"{current_keyword}",
            "safe": "active",
            "cse_tok": "AB-tC_4Ze_YlP_wKRVixeMkkrmtK:1740990064789",
            "lr": "",
            "cr": "",
            "gl": "",
            "filter": "0",
            "sort": "",
            "as_oq": "",
            "as_sitesearch": "",
            "exp": "cc",
            "callback": "google.search.cse.api3877",
            "rurl": "https://search.kompas.com/search/?q=Heavy+rain&submit=%E6%8F%90%E4%BA%A4"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='clearfix']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
