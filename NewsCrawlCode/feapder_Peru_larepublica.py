# 付费
import json
import re
import time
import uuid

import feapder
from feapder import Item


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

    previous_links = None
    country = 'Peru'
    table = 'Peru'
    # 西班牙语
    keywords = ["Ciclón tropical", "Depresión tropical", "Tormenta tropical", "Tifón", "Huracán", "Ciclón", "Tormenta", "Lluvias intensas", "Inundación", "Marea", "Daños costeros", "Deslizamiento", "Desastre geológico", "Desastre marino", "Vientos fuertes", "Desastre de tifones", "Derrumbes de lodo", "Deslizamiento de tierra"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://cse.google.com/cse/element/v1"
            params = {
                "rsz": "filtered_cse",
                "num": "10",
                "hl": "es",
                "source": "gcsc",
                "start": "0",
                "cselibv": "5c8d58cbdc1332a7",
                "cx": "013803619059868835650:ri6kzichbws",
                "q": "calor",
                "safe": "off",
                "cse_tok": "AB-tC_77WAQS0FVuiTIDb9GjFBZA:1741053703782",
                "lr": "",
                "cr": "",
                "gl": "",
                "filter": "0",
                "sort": "date",
                "as_oq": "",
                "as_sitesearch": "",
                "exp": "cc",
                "fexp": "72821495,72821494",
                "callback": "google.search.cse.api12441",
                "rurl": f"https://larepublica.pe/buscador?buscando={keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "referer": "https://larepublica.pe/",
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
            "__Secure-3PSIDTS": "sidts-CjIBmiPuTdxxH6Xb01X7UHquAO1qi0ll-YJl7ZQicoz5mw2YUcxvxSUKtLhh5N-aooWcuRAA",
            "NID": "520=kpHMyOJoYOD6bNr6yAoDdTYff-5WKP4f_5reHt2uT8Sn1albuRemZJlQH4Tlg9GOt3fu0ttE3UQuW-W33DfkBrwXUnCikAfkof_hVnDBkXE1Ybe4VATRr_ZxSimCD2Lp3esoZJmUazQ3K_7EMPDCEqfd32vsVCy1jEepDvITCik7SgLurjsMPUXG-hlh1P0PQA8uehwUZoJigFt98Lj5JYAY-DVblAWvsFxQ3GmAfPcZYo-tn-fSwWGMUfCXwZlaD1897r1idt_VTOXrIOXL_U75P15kmiKFtc_haWx0OZaWDZX9cQ92B5VS1JdrPizwopQ6K57rJX70uOxZGbacepCF4-ArO-G1ApeDJRyS3Vmbl2TKc73Cbmrat4t97FzNSMlue7D3VbMI9R8E0fET6mXfd70zGGlPDVZlYdTJxyx0HwlJ-VTxJicLn2gyfiVCRanmKiIrOVTnr2_cbMTMjADcr8ncgEKNk_Lp66uNVBcitQaHdJbVnwGFS0YAtxrTIyYHofUevv7YVv4Sr8iXSswLd05X306FVnSU2-ihwqc0BoADzZRqxoaQTgOBzHnI-5sz704fpm3fd3qAuOlByRFVlZT1kj0wvn_0Vz_RY_HnrI8K-J1dsDV12CnXGKkq6ymRUZTJf29zTtGa1PAIUIjKa-XALi2G4dmNROAmFBCYN5r72eIz7W0Odg4TBdueew4aVwVp2vIMZ0m9svqE8Rado3qNIdajKuZ255cqPJD1XQO17i7D4pf75o0bs6gbDhe5jKy1_8p2OBmPTCotwF0LjxohWY_bIfYqYs1DGwBftsqz7wd8Pf122yAxGwjhaUMzt_5ulf9fAeub",
            "__Secure-3PSIDCC": "AKEyXzWndVXz8Iv5BbjI1bHtEJ0DWePjq2PL6DHLNk5MeI2xkZN6WpPe3xapL8kgcYQZ5ubJ08M"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = json.loads(response.text.split('api12441(')[-1].split(");")[0])['results']
        # print(json.loads(links))
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
            items.article_url = item.get("url")
            items.title = item.get("title")
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://cse.google.com/cse/element/v1"
        params = {
            "rsz": "filtered_cse",
            "num": "10",
            "hl": "es",
            "source": "gcsc",
            "start": f"{current_page * 10}",
            "cselibv": "5c8d58cbdc1332a7",
            "cx": "013803619059868835650:ri6kzichbws",
            "q": f"{current_keyword}",
            "safe": "off",
            "cse_tok": "AB-tC_77WAQS0FVuiTIDb9GjFBZA:1741053703782",
            "lr": "",
            "cr": "",
            "gl": "",
            "filter": "0",
            "sort": "date",
            "as_oq": "",
            "as_sitesearch": "",
            "exp": "cc",
            "fexp": "72821495,72821494",
            "callback": "google.search.cse.api12441",
            "rurl": f"https://larepublica.pe/buscador?buscando={current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//article//p/text()").extract()).strip().replace("\r", '').replace("\n",
                                                                                                                   '')
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
