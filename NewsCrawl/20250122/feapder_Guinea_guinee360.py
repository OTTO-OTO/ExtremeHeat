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

    country = 'Guinea'
    table = 'Guinea_guinee360'
    keywords = [ "Chaud","Vague de chaleur extrême", "Température élevée", "Température extrême", "Événement de vague de chaleur", "Augmentation de la température élevée", "Impact de la température élevée",  "Chaleur intense", "Augmentation de la température", "Événement de chaleur", "Augmentation de la température", "Pluies intenses", "Précipitations fortes", "Pluie torrentielle", "Pluies extrêmes", "Sécheresse", "Sécheresse sévère", "Sécheresse prolongée", "Pénurie d'eau", "Panne de courant", "Panne de courant par température élevée", "Panne de courant par vague de chaleur", "Panne de courant causée par température élevée", "Incendie", "Incendie par température élevée", "Incendie par chaleur", "Incendie provoqué par température", "Incendie induit par chaleur", "Impact agricole", "Vague de chaleur en agriculture", "Dommages aux cultures", "Stress thermique agricole", "Hypoxie", "Coup de chaleur", "Coup de chaleur induit par chaleur", "Hypoxie par température élevée", "Coup de chaleur par température élevée", "Impact sur le trafic", "Trafic par température élevée", "Trafic par vague de chaleur", "Trafic par température", "Catastrophe écologique", "Catastrophe par chaleur", "Environnement de température élevée", "Impact de la chaleur sur la biodiversité", "Écologie de la vague de chaleur", "Pollution", "Pollution par température élevée", "Pollution par chaleur", "Pollution par température", "Blanchiment des coraux", "Récifs coralliens par température élevée", "Blanchiment des coraux par température" ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.guinee360.com/page/1/"
            params = {
                "s": f"{keyword}"
            }
            yield feapder.Request(url, params=params,callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.guinee360.com/?s=Chaud",
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
            "PHPSESSID": "04dcc443f6cd0c121dd37b41b0e56ce8",
            "_ga": "GA1.1.570084905.1737506736",
            "_ga_836LW6JDN1": "GS1.1.1737506735.1.1.1737506788.0.0.0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//h3/a/@href").extract()
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
            if "video" in item:
                continue
            items = Item()
            print(item)
            items.table_name = self.table # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url =item
            # items.title = item.get("post_title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://www.guinee360.com/page/{current_page}/"
        params = {
            "s": f"{current_keyword}"
        }
        yield feapder.Request(url, params,callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='content-inner ']//p//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
