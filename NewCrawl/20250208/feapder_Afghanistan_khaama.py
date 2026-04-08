# -*- coding: utf-8 -*-
"""

集群运行

"""
import re

import feapder
from feapder import Item
import json
from lxml import etree


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[6, 8],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB="other_country_site3",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Afghanistan'
    table = 'Afghanistan_khaama'
    keywords = [
        "heatwave","Extreme heatwave", "High temperature", "Extreme temperature", "Heatwave event", "Increase in high temperature", "High temperature impact", "Strong heat", "Temperature rise", "Heat event", "Temperature increase", "Heavy rainfall", "Heavy precipitation", "Torrential rain", "Extreme rainfall", "Drought", "Severe drought", "Long-term drought", "Water shortage", "Power outage", "High temperature power outage", "Heatwave power outage", "Power outage caused by high temperature", "Fire", "High temperature fire", "Heat fire", "Temperature fire", "Heat-induced fire", "Agricultural impact", "Heatwave agriculture", "Crop damage", "Agricultural heat stress", "Hypoxia", "Heatstroke", "Heat-induced heatstroke", "High temperature hypoxia", "High temperature heatstroke", "Traffic impact", "High temperature traffic", "Heatwave traffic", "Temperature traffic", "Ecological disaster", "Heat disaster", "High temperature environment", "Impact of heat on biodiversity", "Heatwave ecology", "Pollution", "High temperature pollution", "Heat pollution", "Temperature pollution", "Coral bleaching", "High temperature coral reefs", "Temperature coral bleaching"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.khaama.com/page/1/"
            params = {
                "s": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page,
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.khaama.com/?s=heavy+rain",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0"
        }
        request.cookies = {
            "__gads": "ID=e6e159e1aa251a7d:T=1737006720:RT=1738997478:S=ALNI_MaaqeRHfKlPAymaUb1Km0gXlqcWWQ",
            "__gpi": "UID=00000feca6616c41:T=1737006720:RT=1738997478:S=ALNI_MbhR7NWMKXHPeKFmgKzjDFIxaF0jw",
            "__eoi": "ID=c0e2b13774f64a1c:T=1737006720:RT=1738997478:S=AA-AfjaF4slSu3EFkww8jpMillHA",
            "FCNEC": "%5B%5B%22AKsRol8BQI6KP3xFxpr9u4fzoXBTvahB_E1yBeqH3CnCTOH3XVsO8af2jqvpv9rx3CDjgEoovFerDdNVe9c80-y24Pb5HKx1PmNGoHAiTlEti2MKKGZytFVcjU_N7guY9Ye5jXk30NCaXUV9sStdXHgnqhXxgRSfdw%3D%3D%22%5D%2Cnull%2C%5B%5B2%2C%22%5Bnull%2C%5Bnull%2C4%2C%5B1737006723%2C146216000%5D%5D%5D%22%5D%5D%5D"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        # print(type(response.text))
        # print(type(response.json))
        # links = json.loads(response.text)
        # print(response.text)
        links = response.xpath("//div[@id='tdi_61']//h3/a/@href").extract()
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
            items.table_name = self.table  # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://www.khaama.com/page/{current_page}/"
        params = {
            "s": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='tdb-block-inner td-fix-index']//p/text()").extract())
        items.author = ''
        items.pubtime = ''
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()

