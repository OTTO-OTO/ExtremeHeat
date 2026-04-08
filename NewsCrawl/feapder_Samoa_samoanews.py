import json

import feapder
from feapder import Item
from curl_cffi import requests
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
        MYSQL_DB="spider_data",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Samoa'
    table = 'Samoa_samoanews'
    keywords = ["Extreme heatwave",
                "High temperature", "Extreme temperature", "Heatwave event",
                "Increase in high temperature", "High temperature impact", "Strong heat", "Temperature rise",
                "Heat event", "Temperature increase", "Heavy rainfall", "Heavy precipitation", "Torrential rain",
                "Extreme rainfall", "Drought", "Severe drought", "Long-term drought", "Water shortage", "Power outage",
                "High temperature power outage", "Heatwave power outage", "Power outage caused by high temperature",
                "Fire", "High temperature fire", "Heat fire", "Temperature fire", "Heat-induced fire",
                "Agricultural impact", "Heatwave agriculture", "Crop damage", "Agricultural heat stress", "Hypoxia",
                "Heatstroke", "Heat-induced heatstroke", "High temperature hypoxia", "High temperature heatstroke",
                "Traffic impact", "High temperature traffic", "Heatwave traffic", "Temperature traffic",
                "Ecological disaster", "Heat disaster", "High temperature environment",
                "Impact of heat on biodiversity", "Heatwave ecology", "Pollution", "High temperature pollution",
                "Heat pollution", "Temperature pollution", "Coral bleaching", "High temperature coral reefs",
                "Temperature coral bleaching"
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
                "cselibv": "8fa85d58e016b414",
                "cx": "743bc42005f583535",
                "q": f"{keyword}",
                "safe": "off",
                "cse_tok": "AB-tC_7XGluRi23JCpV6m4bFoUkp:1736404312315",
                "sort": "",
                "exp": "cc,apo",
                "callback": "google.search.cse.api453",
                "rurl": "https://www.samoanews.com/"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "referer": "https://www.samoanews.com/",
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
            "NID": "520=jbGCulqsFUVAQb65GbFkZ8Xd8xP-oEXOy50j-iSfUeTGLq3XExOq37qdTy4YCHHkTF-fJ13w8oWt8HQkT04w9wBdZC7-7cpbEHvhpsR6SPhWa6iSMOeKOw2FqfspPnfgLH0BTlxKEHJGtRkejWqiF-iCQiZnJwmowXgq70jwc8tMi_XR5gwix14hxMBtZtSNA_bCVFMqkyt1AX35HDeAPvajlmWMOuK8OSEVCoYtrK2kKwC-yvWn7tmcvOi4_579s4hDeWzM81cCx4Dpy4MVOp922MDC6P8cwt2mj4P1X3Q9xZpNKic1uGPwu5lQ2OOXuUTGvnEDXVHz3vngN6qYWJ3tX_05Eh6ZYN3_n65BWpgljiJfdxtTCthi4wubsEq--irfTbi5e-9O3gNFhc5QNFJ8Q-RWl844QvsSrYpky-O6Mmvy5ySNvZiuWetgrJ6h5I1WxavwCkjF_EYgWE6mdow8O3Q50WhWVCpMFcUe9tiCaCuk2tlxh44e7Kz0jHOdFYJDkAPi_dKngdypdByEMFrYpap5lJeE6nqUUyKv5ZRoS8hIAZBwxxsJl5s-QG6i-eu2if4HgbtEhVQ_agAsp4mRylrLjbrsKwp4X4D0v4kdPJtWDLomDvAY25P8FsZUSm0OjUmvCYx_gDTTUU5TVo8NhPPkktYS9Z_xaJXthcfjWhiqWKsBQUtZf5wGY39k2MuHgCOFPPLnjZrgi55IpjmkECnoGNugLyFweqvreN5h8U1Ijz7MihbpjPsPuyKFIIlKRb-arjhxriAEONKXOUnJjTOdik5EOgPHqBxZ1w",
            "__Secure-3PSIDCC": "AKEyXzVmDzNp6_aafgH6pz7AvSNm1lnGZsyF8qVv0kmN-2Fv5TQsINPyX2HHO9d_FHxXfvoJLlc"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        data = response.text.split("api453(")[-1].split(");")[0]
        # print(data)
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
            print(item.get("url"))
            items = Item()
            items.table_name = self.table
            items.article_url = item.get("url")
            items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://cse.google.com/cse/element/v1"
        params = {
            "rsz": "filtered_cse",
            "num": "10",
            "hl": "en",
            "source": "gcsc",
            "start": f"{current_page * 10}",
            "cselibv": "8fa85d58e016b414",
            "cx": "743bc42005f583535",
            "q": f"{current_keyword}",
            "safe": "off",
            "cse_tok": "AB-tC_7XGluRi23JCpV6m4bFoUkp:1736404312315",
            "sort": "",
            "exp": "cc,apo",
            "callback": "google.search.cse.api453",
            "rurl": "https://www.samoanews.com/"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        # items.title = response.xpath("//title/text()").extract_first()
        next_url = response.xpath("//span[@class='field-content']/a/@href").extract_first()
        resp = requests.get(next_url).text
        html = etree.HTML(resp)
        content = "".join(html.xpath("//div[@class='content']//p/text()"))
        items.content = content
        items.author = ''
        items.pubtime = ''
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
