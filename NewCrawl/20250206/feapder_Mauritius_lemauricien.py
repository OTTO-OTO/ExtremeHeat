# -*- coding: utf-8 -*-
"""

本地运行

"""
import re

import feapder
from feapder import Item
import json
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
    previous_links = None

    country = 'Mauritius'
    table = 'Mauritius'
    keywords = ["Cyclone tropical", "Dépression tropicale", "Tempête tropicale", "Typhon", "Ouragan", "Cyclone", "Tempête", "Pluies diluviennes", "Inondation", "Houle", "Dommages côtiers", "Glissement", "Catastrophe géologique", "Catastrophe marine", "Vents violents", "Catastrophe de typhon", "Glissement de terrain", "Landslide"]

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
                "cx": "a84263cd2de838033",
                "q": f"{keyword}",
                "safe": "off",
                "cse_tok": "AB-tC_5KnnDLOrFFaz3HL0h3sg4E:1740619323934",
                "lr": "",
                "cr": "",
                "gl": "",
                "filter": "0",
                "sort": "",
                "as_oq": "",
                "as_sitesearch": "",
                "exp": "cc",
                "callback": "google.search.cse.api2847",
                "rurl": "https://www.lemauricien.com/recherche/"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "referer": "https://www.lemauricien.com/",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "script",
            "sec-fetch-mode": "no-cors",
            "sec-fetch-site": "cross-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0"
        }
        request.cookies = {
            "__Secure-3PSID": "g.a000rggyq3rN6l5a4RzrqfKR8L6w_4oSKSt2-NpZPRMnn-bzQbIYlvrEO6wyUoA2X7nR4dKaLQACgYKAc8SARcSFQHGX2Mi1LRifzVo7z1Yh0GO1dLi3BoVAUF8yKq57cjKXq68AYkR7eUXDFJW0076",
            "__Secure-3PAPISID": "IMadT4_VMCUqIjYe/AiQnZJDT7r7FsfPRc",
            "__Secure-3PSIDTS": "sidts-CjIBmiPuTVoGkIfi9mvTUVCKD4hmvWP9DC0RsXoi3RWh47P6hfQWJqQPKo0a3SsC9ZUW7RAA",
            "NID": "521=Xuqee65BKVQeWjm_Ejbu7gDWmfcJe2u0652olRdPUtNBIU3nCWIm9cmGONt2DSIpRtti2nSeWdGvWKUVs9TTojf9nN080VoMZeVM3_PXS_Azo95exlAYeShxJ4Q4FBUCuQdUoDYn16l_WEsR36UpzmoVYsKTRp8_Yf7gstBsdEtd78EENdEqypEg9_BfHgeU5O28GIa8ekWhca9dCT9izcBrGrNUEv4QTfk5kl_wTJgXx3nPKknY9lzo3Z-vZDr5L6bR-FxZgeHxqzY1jAUc1UeX5Nm6T94sge0gzVaxoZnaq787jBmIhB9iOOTgYRlrDUHJPyX1SvW9mMr_Z8bNzT3w4fonSLr5TqINfQ4KojIkLeCysejck5aESOHW-cp8UqSy-43A7ZF_eci8ka3trd5lQlCCmCe_DfL3Z4ER8jvsLl8DMur3kkZooeDHUq2YKxcqJFfcmHbDt33XBKB9iIr5FOgoZuejh8Kp0pAG5l8XVRSQ-b2xLtfJEG_VkbjSlQAv2sJCn9_iY9NFQA8uFZJMMOW5BESv4kDDpJraMlBRZdrQqf7BOZ70LaSkvIIOU4Et3G2Q8UZNTk3xg4noGizcT5qIPhCrhvFACXBNX6IbHj6F6PL74pVoeie6gk7OO33xr_5qqTyS6qPrVp_tO27EHHmBw4yOZYyY6N_vUTpkZF6Qf08zRDmi2XZy22BR14cnJM0f7UL_vNiJrc3BzObFcxDwATvCKKJjcMPDcHI3WdeVp4W-17tYC-Re3HCU8GsQlmhEzab6If154Nr5AxJMVby_rAOUVcp6ohWaY8y9IxMScTn6Kg7NqGLPol9dbEd8Bc_WLSgXw3Vb",
            "__Secure-3PSIDCC": "AKEyXzUuVkHij73uagWqD57r4zYnLsmNpdr42bqlGzVPe2c8vAlUIh8ZSyeuhItlgrZhb1SfPno"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = json.loads(response.text.split("api2847(")[-1].split(");")[0])['results']

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
        if request.page >= 10:
            print(f"关键词 {current_keyword} 的第 {request.page} 页已经抓取完毕，退出当前关键字的循环")
            return None
        for item in links:
            items = Item()
            items.table_name = self.table  # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url = item.get("url")
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
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
            "cx": "a84263cd2de838033",
            "q": f"{current_keyword}",
            "safe": "off",
            "cse_tok": "AB-tC_5KnnDLOrFFaz3HL0h3sg4E:1740619323934",
            "lr": "",
            "cr": "",
            "gl": "",
            "filter": "0",
            "sort": "",
            "as_oq": "",
            "as_sitesearch": "",
            "exp": "cc",
            "callback": "google.search.cse.api2847",
            "rurl": "https://www.lemauricien.com/recherche/"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='tdb-block-inner td-fix-index']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
