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

    country = 'Qatar'
    table = 'Qatar'
    #英语
    keywords = ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.gulf-times.com/search"
            params = {
                "query": f"{keyword}",
                "pgno": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=1, i",
            "referer": "https://expresso.pt/procura?q=Temperatura+alta",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0"
        }
        request.cookies = {
            "gig_bootstrap_3_SlQdOiQ8k-7grQ_Mx0LfKus2fvYJ7ge7QRBxXcLAk0x_hhvxLLYiLrGZwQrLpUlW": "_gigya_ver4",
            "didomi_token": "eyJ1c2VyX2lkIjoiMTk0ZGE0NDYtOTdiMC02OWRhLTllMzktYTU1YmYyN2M3ZjliIiwiY3JlYXRlZCI6IjIwMjUtMDItMDZUMDc6NTg6MjQuMTIzWiIsInVwZGF0ZWQiOiIyMDI1LTAyLTA2VDA3OjU5OjA1LjkyOFoiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwidHdpdHRlciIsImM6YWRtZWRpYSIsImM6aHVic3BvdCIsImM6cm9ja2V0LWZ1ZWwiLCJjOmFkdmVydGlzaW5nY29tIiwiYzphdGFuZHQtYWR3b3JrcyIsImM6eWFob28tYWQtZXhjaGFuZ2UiLCJjOnlhaG9vLWFuYWx5dGljcyIsImM6aG90amFyIiwiYzp5YWhvby1hZC1tYW5hZ2VyLXBsdXMiLCJjOnNpdGVzY291dCIsImM6Y2hhcnRiZWF0IiwiYzpiZXR3ZWVuLWRpZ2l0YWwiLCJjOmxvbmd0YWlsLXZpZGVvLWFuYWx5dGljcyIsImM6aW5zaWRlIiwiYzpodWJzcG90LWZvcm1zIiwiYzpnb29kZWVkLXdQWXdVRlY3IiwiYzpnb29nbGVhbmEtNFRYbkppZ1IiLCJjOmF3cy1jbG91ZGZyb250IiwiYzphZHNjYWxlIiwiYzpqdy1wbGF5ZXIiLCJjOmFkbWV0cmljcy03N25aWFdiOSJdfSwicHVycG9zZXMiOnsiZW5hYmxlZCI6WyJkZXZpY2VfY2hhcmFjdGVyaXN0aWNzIiwiZ2VvbG9jYXRpb25fZGF0YSJdfSwidmVuZG9yc19saSI6eyJlbmFibGVkIjpbImdvb2dsZSJdfSwidmVyc2lvbiI6MiwiYWMiOiJERk9BNEFFWUFMb0FlZ0NiZ0UtQUtPQWpBQkh3RFlBTVJBWXBBMGNCclFEY29IVGdPckFnWUJCdUNhMEZNZ0xRd1dqQXRWQmJJQzRNRnhnTHl3WXBnQUFBLkRGT0E0QUVZQUxvQWVnQ2JnRS1BS09BakFCSHdEWUFNUkFZcEEwY0JyUURjb0hUZ09yQWdZQkJ1Q2EwRk1nTFF3V2pBdFZCYklDNE1GeGdMeXdZcGdBQUEifQ==",
            "euconsent-v2": "CQMZ9MAQMZ9MAAHABBENBbFsAP_gAEPgAB6YKytX_G__bWlr8X73aftkeY1P99h77sQxBhbJE-4FzLvW_JwXx2E5NAz6tqIKmRIAu3TBIQNlHJDURVCgaogVrSDMaEyUoTNKJ6BkiFMRI2dYCFxvm4tjeQCY5vr991dx2B-t7dr83dzyy4hHn3a5_2S0WJCdA4-tDfv9bROb-9IOd_x8v4v4_F7pE2_eT1l_tWvp7D9-cts_9XW99_fbff9Pn_-uB_-_X_vf_H37oKyAEmGhUQBlgSEhBoGEECAFQVhARQIAgAASBogIATBgU7AwAXWEiAEAKAAYIAQAAgyABAAABAAhEAEABQIAAIBAoAAwAIBgIACBgABABYCAQAAgOgYpgQQCBYAJGZFQpgQgAJBAS2VCCQBAgrhCEWeARAIiYKAAAAAApAAEBYLA4kkBKhIIAuIJoAACABAIIAChBJyYAAgDNlqDwZNoytMAwfMEiGmAZAEQRkJBoQAA.f_wACHwAAAAA",
            "_cb": "D8TzNnDUOcRAB_ZChi",
            "_cb_svref": "external",
            "browser_id": "6e0c8c9e-5250-4a2f-b331-8b1056c0c11b",
            "_gid": "GA1.2.370735788.1738828747",
            "__gfp_64b": "Imh8LwOUOWdPIWuFIrG.ca6lqKwzYsD303mePVQEP0P.w7|1738828729|2|||8:1:80",
            "_hjSession_1732642": "eyJpZCI6ImFkOWQyZGQ3LWFiYzItNDQ0My05OTI0LWY0NzAyYTRlZjk3YiIsImMiOjE3Mzg4Mjg3NTE1ODEsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MH0=",
            "q": "Temperatura alta",
            "_gat": "1",
            "_chartbeat2": ".1738828747423.1738828846327.1.Bj-BNnBuPhLyK39fwD_qkwGCiR0yD.2",
            "_ga": "GA1.1.1480088835.1738828747",
            "_hjSessionUser_1732642": "eyJpZCI6IjBjNzk0YThmLTg4OTQtNTI0OC05YzI4LTQ0ZDgwNzViMTM5NSIsImNyZWF0ZWQiOjE3Mzg4Mjg3NTE1ODAsImV4aXN0aW5nIjp0cnVlfQ==",
            "_fbp": "fb.1.1738828848189.522626263197061573",
            "__eoi": "ID=d47086c52bdee210:T=1738828827:RT=1738828827:S=AA-Afja07JpPW0zJXqfacncqaglj",
            "FCNEC": "%5B%5B%22AKsRol8xYWKnLTPhyK_lRtBZZ1lnWLU-hJ6Y_Dz3lWNsreKSOTJDtkuL9FCnrngCTsI0R8IVt4GiuChueCkz__2nYlukrK1jw93U1CrjmzK2euK9EJIoloBodmtCGAOuRZ0_Tk9bK3a2thJNOF1853vYuj2ZEJYJlA%3D%3D%22%5D%5D",
            "_ga_EWF24SYQSZ": "GS1.1.1738828748.1.1.1738828849.54.0.0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//h2/a/@href").extract()
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
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.gulf-times.com/search"
        params = {
            "query": f"{current_keyword}",
            "pgno": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='article-body ']//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
