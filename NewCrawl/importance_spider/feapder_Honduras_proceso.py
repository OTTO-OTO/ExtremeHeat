import re

import feapder
from NewsItems import SpiderDataItem
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
        MYSQL_DB="TropicalCyclone",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )

    country = 'Honduras'
    table = 'Honduras'
    # 西班牙语
    keywords = ["Ciclón tropical", "Depresión tropical", "Tormenta tropical", "Tifón", "Huracán", "Ciclón", "Tormenta", "Lluvias intensas", "Inundación", "Marea", "Daños costeros", "Deslizamiento", "Desastre geológico", "Desastre marino", "Vientos fuertes", "Desastre de tifones", "Derrumbes de lodo", "Deslizamiento de tierra"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://cse.google.com/cse/element/v1"
            params = {
                "rsz": "filtered_cse",
                "num": "10",
                "hl": "en",
                "source": "gcsc",
                "cselibv": "5c8d58cbdc1332a7",
                "cx": "f7fcaecd7493f4f39",
                "start":"0",
                "q": f"{keyword}",
                "safe": "off",
                "cse_tok": "AB-tC_4dudpjQ3FsLmAgOAtWUJEB:1740986807808",
                "lr": "",
                "cr": "",
                "gl": "",
                "filter": "0",
                "sort": "",
                "as_oq": "",
                "as_sitesearch": "",
                "exp": "cc,apo",
                "oq": "Ciclón tropical",
                "gs_l": "partner-web.12..0i512l3j0i22i30l7.2761.2761.0.5247.1.1.0.0.0.0.252.252.2-1.1.0.csems,nrl=10...0....1.34.partner-web..0.1.252.do6xTTXgC1A",
                "callback": "google.search.cse.api3209",
                "rurl": "https://proceso.hn/tdb_templates/search/"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "referer": "https://proceso.hn/",
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
            "NID": "520=qlKQMvKBdgiKvslSrJAEsrQBAOv0l3ZaNziHMOV7z8Qu59oOpX8fwchcJQ3eSdNYwiKuZdTXDhSnAn18dJF2_hO6T3UVLQuYoIUh5QXLkePKdm6IygmRtWDkZOcpTLTXwWIZIB3Ld_6MJz66uG2kaQ3wmKoDn52KAychEC0d5qbmJVd1xane5hl5eMSXRoj37-bhrV1FsMMGpNy8oNlw1sr_I22Z5KWYK362NfIfHE_7xvomiPjnJT9vrTJrsAV-6ZOTryBiruXgxeaAcUGPsK1a8ryfWQ946ca_bAKGvzMTIhTxXp39B3gICoKS_9AwVoi5Km4PfK56C3o_DQRFANIG_FfkQHSy_0RSTaiDx1xFHkBur9hdAO43en6mn0tV7wYKOSnqMS1l9TkjnCK11Fn7dbNayt66yXBDVyjjnbAqmM4ha_8EHBXoIv-DEoRJv4Zr4JEhvuWd8gq4UdXgzLIA9wiM0QtraWw5aZhMke6VaD1rGkKXAzKOsseu5hppZwyyyz11ff50Y1Rpa1GXJSXv_haMZt13atWmTk5vHUnAkUm_iLARod3t6EBeq8GCiVsbzKDAnE0In4lMWNEaDwlOELGsHYL4hHOfjJxusBScofJzsXZiPhj7xmfrFp21uWVg4Tzw44z7ECossU8OHCST_gfXZXdq88-47ccZ_LARTHFxKcYRcv7Uc2GNFeLvUZKQk4pbkC-Ip4XG_5IvAx5gVOgGJj5IMvfMD7w5vvl5KlD3EdVv6KJguDnzqySd5Fz7nBbvIKxJvU_PSzlcfnuHK81noKZXw16J7t5HqQ",
            "__Secure-3PSIDTS": "sidts-CjIB7wV3sZ_WCRgS-4Eo-OfDw1LVWW6xLN412SrHzKTY_1NtCcWRsa4lQ__XCEtox88CqxAA",
            "__Secure-3PSIDCC": "AKEyXzU25TkWX-qF1a90FuZoizT-COKuWeHpDM35grYjGlxOKePqzw6KIUDloLXCYCldu2TBwSk"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        # print(response.text)
        data = response.text.split("api3209(")[-1].split(");")[0]
        pattern = r'"url": "(.*?)"'
        links = re.findall(pattern, data)
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            print(item)
            items = SpiderDataItem()
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
            "start":f"{current_page}",
            "cselibv": "5c8d58cbdc1332a7",
            "cx": "f7fcaecd7493f4f39",
            "q": "Ciclón tropical",
            "safe": "off",
            "cse_tok": "AB-tC_4dudpjQ3FsLmAgOAtWUJEB:1740986807808",
            "lr": "",
            "cr": "",
            "gl": "",
            "filter": "0",
            "sort": "",
            "as_oq": "",
            "as_sitesearch": "",
            "exp": "cc,apo",
            "oq": "Ciclón tropical",
            "gs_l": "partner-web.12..0i512l3j0i22i30l7.2761.2761.0.5247.1.1.0.0.0.0.252.252.2-1.1.0.csems,nrl=10...0....1.34.partner-web..0.1.252.do6xTTXgC1A",
            "callback": "google.search.cse.api3209",
            "rurl": "https://proceso.hn/tdb_templates/search/"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='tdb-block-inner td-fix-index']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
