# -*- coding: utf-8 -*-
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
    country = 'El Salvador'
    table = 'El_Salvador'
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
                "cx": "006989654994863071413:2meag52dzuc",
                "q": {keyword},
                "safe": "off",
                "cse_tok": "AB-tC_6tEpMzSvcvMoN0MHa1n7--:1740984266595",
                "sort": "date",
                "exp": "cc",
                "callback": "google.search.cse.api16434",
                "rurl": "https://www.elsalvador.com/"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "referer": "https://www.elsalvador.com/",
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
            "NID": "520=VjBOCSDaodDeQ3ZV8IMw0M3pA7TrnY6BwuUKRMI70xtYzopZyzRQsuQHnjPFNv2V5uabEP6Cg_WxKBUfF4ckaK4Il2hh7eoYHKuRi8rbFTiZBQY_wcXHq3g_NVKlcYLlz9xm7ZtIw4qVaWdbD8eFti_27hvgAKd7l3he2Ch-hVUBwJP5NNl4ZvvP_NixbbF8O_T0pU0Z1s_R41rgY3sN_zlVXLAnwNm2WnA6zWHMSD2h19XyZ-oU6OnLMQWb-uAEV3cF7Jr30ifA7nzXK1mCSCdSDkaIuumBb7xlHh1BixmK9xDF9GOgzVwU1A6STTkA5kjc4KjtZFzKOukrj1PEKqdUwAWhSPyIViD2pGh-p0DpUXcm0vVEOanPVaKaPhsFGEB-swmFNKt3wW-_k31qouEGzkjXmU7ZvgaaJuw5snC1AU85ouHGHye22kxDGI6jYzPUpK8yIAKgseYiEx9hYMaFVSK4m0ZJOEoGKlxfzVmX8SGNo08cW_w8kWXlXuHLszOs8znfNoYpp_D4DmacQipnk-HbC1tTK0Rr_eU3Na52P1aiQzWZO286sKMwwgJi7U3G9fFauuB9WSaCbhj8L_gB5R5ZFVWsT1tZZsd72Z9Z1X6z6qosDRBbAb_WiAys9Mg_ik8h4s-uYhQjtc6Ms37aVz0hzxhnx2EE3wnCeDEICI7EUvh3BQbiahLMN48HbEp5_MjD60w2NZ-NJq2-A1DorlPIIO0Xcz0-bhJqcJ9YqIAYWqUa_h5gXtfbeGu3PJRd1ol8T48KM6vPQefSymNo51lqKo8pX9tps1vOAUIzoT8sNyuFNkAJySqewxfITuQY2Xc9aL9HJo17",
            "__Secure-3PSIDCC": "AKEyXzV1Ah83N1B0m4q2gxCuCZkJTUkbaT2Kz2K-2qv_PQSnvBkWeX_6J98xzAn_o0dMOhPeOxQ"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = json.loads(response.text.split("api16434(")[-1].split(");")[0])['results']
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
            # items.pubtime = item.get("recs-publishtime")
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 10
        url = "https://cse.google.com/cse/element/v1"
        params = {
            "rsz": "filtered_cse",
            "num": "10",
            "hl": "es",
            "source": "gcsc",
            "start": f"{current_page}",
            "cselibv": "5c8d58cbdc1332a7",
            "cx": "006989654994863071413:2meag52dzuc",
            "q": f"{current_keyword}",
            "safe": "off",
            "cse_tok": "AB-tC_6tEpMzSvcvMoN0MHa1n7--:1740984266595",
            "sort": "date",
            "exp": "cc",
            "callback": "google.search.cse.api16434",
            "rurl": "https://www.elsalvador.com/"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='entry-content']//p/text()").extract()).strip()
        items.author = ''
        items.pubtime = response.xpath("//span[@class='ago']/span/span/text()").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
