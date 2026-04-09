import json

import feapder
from feapder import Item
from curl_cffi import requests
from lxml import etree
import re


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[6, 10],
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

    country = 'Seychelles'
    table = 'Seychelles'
    #英语
    keywords = ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "http://www.seychellesnewsagency.com/search_news"
            params = {
                "do_search": "no",
                "thequery": f"{keyword}",
                "": "",
                "s": "0"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.blic.rs/search?q=ekstremna+temperatura",
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
            "acc_segment": "53",
            "acc_segment_ts": "1736409804",
            "_sotmsid": "0:m5p1kofg:dqSv8qONp0Dh8NDZkyKFXJQf7~qjj0z0",
            "_sotmpid": "0:m5p1kofg:O9w1R_69FYMaauYxyg9NWhiEJkf_I56x",
            "_ain_cid": "1736409821185.918306101.8860257",
            "_ain_uid": "1736409821185.111045800.18012711",
            "_gcl_au": "1.1.193785026.1736409822",
            "_clck": "1dylys6%7C2%7Cfsf%7C0%7C1835",
            "___nrbi": "%7B%22firstVisit%22%3A1736409823%2C%22userId%22%3A%22403ad754-2aa3-45f1-8c69-a81f2363f99e%22%2C%22userVars%22%3A%5B%5D%2C%22futurePreviousVisit%22%3A1736409823%2C%22timesVisited%22%3A1%7D",
            "compass_uid": "403ad754-2aa3-45f1-8c69-a81f2363f99e",
            "_ga": "GA1.1.19721736.1736409825",
            "__gfp_64b": "g0Vg1KK_WeApfkiZjU9_j3ZLGt2I0V.xY0RGYcINY2H.U7|1736409815|2|||8:1:80",
            "am-uid-f": "f71c8005-9fd6-4a9d-9f15-4222e33c5020",
            "_hjSession_2355555": "eyJpZCI6ImQzMzAxN2NjLTJlNDItNGY0OC1hMWM4LTg3NGE1OWJhZTdiMSIsImMiOjE3MzY0MDk4MzI4NjAsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MH0=",
            "_sharedid": "5d968265-8640-431c-a447-177ef89ead88",
            "_sharedid_cst": "aCzxLIMsJg%3D%3D",
            "_fbp": "fb.1.1736409835085.997671123997698480",
            "ea_uuid": "202501090904243098208410",
            "ats_ri": "ri=202501090904243098208410&model=202501090904243098208410&models=eyJzcmMiOiJndSIsImF0c19yaSI6IjIwMjUwMTA5MDkwNDI0MzA5ODIwODQxMCJ9&ttl_ms=3600000&expires_ms=1736413475515&version=1736409864.722",
            "panoramaId_expiry": "1737014678686",
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "panoramaId": "3d6e8b78d1ec91ae6e29e218acf4185ca02c60046cf65798b2f03d4a5c35863c",
            "_hjSessionUser_2355555": "eyJpZCI6Ijk3ZThlMjYxLThiZDEtNWIzZS1iZmVlLTA1ZjRhYThkOTVlZSIsImNyZWF0ZWQiOjE3MzY0MDk4MzI4NTksImV4aXN0aW5nIjp0cnVlfQ==",
            "___nrbic": "%7B%22isNewUser%22%3Atrue%2C%22previousVisit%22%3A1736409823%2C%22currentVisitStarted%22%3A1736409823%2C%22sessionId%22%3A%223a3f65f9-b11a-499a-93f6-1ec9c702df18%22%2C%22sessionVars%22%3A%5B%5D%2C%22visitedInThisSession%22%3Atrue%2C%22pagesViewed%22%3A5%2C%22landingPage%22%3A%22https%3A//www.blic.rs/%22%2C%22referrer%22%3A%22https%3A//docs.qq.com/%22%2C%22lpti%22%3Anull%7D",
            "_clsk": "1ce8pgh%7C1736410018768%7C5%7C1%7Cu.clarity.ms%2Fcollect",
            "_ga_M3KRHVEN03": "GS1.1.1736409825.1.1.1736410021.55.0.0",
            "_tfpvi": "ZmJkODZkMTgtZDViZi00NzgwLWI2Y2QtNGY5NWNmZGNjYzM5Iy04LTM%3D",
            "cto_bundle": "3ZSE4V9PT1JQUkJIcFlMNFJhcloxd0pnNGdVM1diUjBxUkhPJTJGQ3I3JTJGVSUyQkp6S29iVzVqYXhVb3d3MWolMkY0S0YlMkJOV2UwMW9qTWJRemp5YXprSDBUdmRsNmp4UFRlSmhEWncySEJ5MTFvem1sa0lteEFuYjc2VHloU0w2JTJCQzI3MXJwSHpFdkREQWxJJTJGOE9zc24xWEdVZ3pJQkdGZyUzRCUzRA",
            "cto_bidid": "CTm4jF9teXhYN2tzMFJyU2xHZ0VvOW9oR051JTJGY1FmTXZqWjM4Vm10dnBHS0NxT1phdTVIcmVFa1AlMkJhZWlkamhVUG5IdkxFJTJCNzljMFZvekY3OWlITHFHd1E5SlVkb0dQOHJoczhYck9BeWJiUWZuRSUzRA",
            "cto_dna_bundle": "5ie3vl9PT1JQUkJIcFlMNFJhcloxd0pnNGdSZjV4Y3BGUGg5dk5mZU01djluS0glMkJKUzQ4QmYzM2xMTm9rVk9LbEVQaHN1S1o5WVdtTUxQbVRTamxUWXhiM0pnJTNEJTNE"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//article//h1/a/@href").extract()
        # print(data)

        # print(links)
        current_page = request.page

        current_links = links
        if self.previous_links is not None and current_links == self.previous_links:
            print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理

        self.previous_links = current_links  # 更新上一页的链接列表
        if request.page >= 10:
            print(f"关键词 {current_keyword} 的第 {request.page} 页已经抓取完毕，退出当前关键字的循环")
            return None
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            # print(item.get("url"))
            items = Item()
            items.table_name = self.table
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 15
        url = "http://www.seychellesnewsagency.com/search_news"
        params = {
            "do_search": "no",
            "thequery": f"{current_keyword}",
            "": "",
            "s": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        content = "".join(response.xpath("//div[@id='textsize']//p/text()").extract())
        items.content = content
        items.author = ''
        items.pubtime = response.xpath("(//div[@class='date']//text())[4]").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
