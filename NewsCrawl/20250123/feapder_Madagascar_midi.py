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

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[8, 10],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB="other_country_site2",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Madagascar'
    table = 'Madagascar_midi'
    keywords = ["Chaud ", "Vague chaleur extrême", "Température élevée", "Température extrême", "Événement vague chaleur", "Augmentation température élevée", "Impact température élevée", "Température élevée", "Chaleur intense", "Augmentation température", "Événement chaleur", "Augmentation température", "Pluies intenses", "Précipitations fortes", "Pluie torrentielle", "Pluies extrêmes", "Sécheresse", "Sécheresse sévère", "Sécheresse prolongée", "Pénurie eau", "Panne courant", "Panne courant température élevée", "Panne courant vague chaleur", "Panne courant causée température élevée", "Incendie", "Incendie température élevée", "Incendie chaleur", "Incendie provoquétempérature", "Incendie induit chaleur", "Impact agricole", "Vague chaleur agriculture", "Dommages cultures", "Stress thermique agricole", "Hypoxie", "Coup chaleur", "Coup chaleur chaleur", "Hypoxie température élevée", "Coup chaleur température élevée", "Impact trafic", "Trafic température élevée", "Trafic vague chaleur", "Trafic température", "Catastrophe écologique", "Catastrophe chaleur", "Environnement température élevée", "Impact chaleur biodiversité", "Écologie vague chaleur", "Pollution", "Pollution température élevée", "Pollution chaleur", "Pollution température", "Blanchiment coraux", "Récifs coralliens température élevée", "Blanchiment coraux température" ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://midi-madagasikara.mg/page/1/"
            params = {
                "s": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://midi-madagasikara.mg/?s=Chaud+",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "advanced_ads_visitor": "%7B%22browser_width%22%3A1857%7D",
            "__gads": "ID=9d7517c89e3acf6a:T=1737618854:RT=1737618854:S=ALNI_MbPL5qmTyzrwyEqaagG8u6iU8o0Jw",
            "__gpi": "UID=000010067372ac84:T=1737618854:RT=1737618854:S=ALNI_MYkcDa7Bp0ZSN81r9BzGmYdGnqrMw",
            "__eoi": "ID=f21e6bd9987d2603:T=1737618854:RT=1737618854:S=AA-AfjbIFtVtMvi5_CLWJniG2_2P",
            "cmplz_consented_services": "",
            "cmplz_policy_id": "2",
            "cmplz_marketing": "allow",
            "cmplz_statistics": "allow",
            "_ga": "GA1.1.2114671138.1737618879",
            "cmplz_preferences": "allow",
            "cmplz_functional": "allow",
            "cmplz_banner-status": "dismissed",
            "advads_procfp_google-auto-placed": "%7B%22count%22%3A6%2C%22exp%22%3A%22expires%3DThu%2C%2023%20Jan%202025%2010%3A54%3A40%20GMT%22%7D",
            "_ga_3KR0YH9WZY": "GS1.1.1737618878.1.1.1737619030.0.0.0",
            "FCNEC": "%5B%5B%22AKsRol9IvYI-IYed5NRGlji2Wag0NPY1R1Yq0FKrQP1QLH6GU6Xv1r3WGcO4NQAMcOfNe8BCU7SR6DegSxiavgxMHzWlHdJuNGwsXcNB4f6MItJhl_YyJFOimCbLdY1G3S5w-pPuZ-v85T-lTxYyZJdWWaDO3YdBBw%3D%3D%22%5D%5D"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//div[@id='tdi_64']//h3/a/@href").extract()

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
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://midi-madagasikara.mg/page/{current_page}/"
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
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
