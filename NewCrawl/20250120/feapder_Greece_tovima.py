# -*- coding: utf-8 -*-
# 173
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
        MYSQL_DB="other_country_site1",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Greece'
    table = 'Greece_tovima'
    keywords = [
    "Ζέστη", "Εξαιρετική καύσιση", "Υψηλή θερμοκρασία", "Εξαιρετικές θερμοκρασίες", "Καύσιση", "Αύξηση υψηλών θερμοκρασιών",
    "Επίδραση υψηλών θερμοκρασιών", "Υψηλή θερμοκρασία", "Ισχυρή ζέστη", "Αύξηση θερμοκρασίας", "Ζεστό γεγονός",
    "Αύξηση θερμοκρασίας", "Ισχυρή βροχή", "Ισχυρή χύση", "Βροχοπτώση", "Εξαιρετική βροχή", "Χρόνια ξηράς", "Σοβαρή ξηράς",
    "Μακροχρόνια ξηράς", "Ελλιπή ύδατος", "Αποψίλωση", "Αποψίλωση λόγω ζέστης", "Αποψίλωση λόγω καύσισης",
    "Αποψίλωση λόγω υψηλών θερμοκρασιών", "Πυρκαγιά", "Πυρκαγιά λόγω υψηλών θερμοκρασιών", "Ζεστή πυρκαγιά",
    "Πυρκαγιά λόγω θερμοκρασίας", "Πυρκαγιά λόγω υψηλών θερμοκρασιών", "Επίδραση στην γεωργία", "Καύσιση στην γεωργία",
    "Καύσιση καλλιέργησης", "Αγροτική ζέστη", "Ανοξία", "Ανοξία λόγω ζέστης", "Ανοξία λόγω υψηλών θερμοκρασιών",
    "Επίδραση στην κίνηση", "Ζέστη στην κίνηση", "Καύσιση στην κίνηση", "Θερμοκρασία στην κίνηση", "Οικολογική καταστροφή",
    "Καύσιση καταστροφή", "Περιβάλλον υψηλών θερμοκρασιών", "Επίδραση ζέστης στη βιοποικιλότητα", "Καύσιση οικοσυστήματος",
    "Παράγωγα", "Παράγωγα λόγω υψηλών θερμοκρασιών", "Ζέστη παράγωγα", "Θερμοκρασία παράγωγα", "Λευκάνιση σαντουριών",
    "Υψηλή θερμοκρασία σαντουριών", "Θερμοκρασία λευκάνιση σαντουριών"
]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = f"https://www.tovima.gr/search/{keyword}/page/1/"
            yield feapder.Request(url, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.tovima.gr/search/%CE%98%CE%B5%CF%81%CE%BC%CF%8C%CF%82",
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
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "_ga": "GA1.1.228728445.1735888446",
            "_cb": "Dn7JJkDtGXVgD4quhs",
            "_fbp": "fb.1.1735888446764.744582175606836739",
            "compass_uid": "6766bb16-ba8c-4ea4-bdc5-c461952139d9",
            "panoramaId": "c2ba4404dd1662779c299379ee5816d53938f08b6896985ba839d86735aa250d",
            "panoramaIdType": "panoIndiv",
            "___nrbi": "%7B%22firstVisit%22%3A1735888448%2C%22userId%22%3A%226766bb16-ba8c-4ea4-bdc5-c461952139d9%22%2C%22userVars%22%3A%5B%5D%2C%22futurePreviousVisit%22%3A1737341047%2C%22timesVisited%22%3A3%7D",
            "_t_tests": "eyI0OENZemNxTkk4UHRVIjp7ImNob3NlblZhcmlhbnQiOiJCIiwic3BlY2lmaWNMb2NhdGlvbiI6WyJCckVEYVMiXX0sImxpZnRfZXhwIjoibSJ9",
            "_cb_svref": "external",
            "___nrbic": "%7B%22previousVisit%22%3A1737189422%2C%22currentVisitStarted%22%3A1737341047%2C%22sessionId%22%3A%2286a0e5f9-0be4-4050-aef4-7517d6f3de8f%22%2C%22sessionVars%22%3A%5B%5D%2C%22visitedInThisSession%22%3Atrue%2C%22pagesViewed%22%3A3%2C%22landingPage%22%3A%22https%3A//www.tovima.gr/%22%2C%22referrer%22%3A%22%22%2C%22lpti%22%3Anull%7D",
            "_chartbeat2": ".1735888445790.1737341275265.0000000000000101.DSXmnLCxWzreu8lCUDKagR7Bk1nUG.3",
            "panoramaId_expiry": "1737946061048",
            "FCNEC": "%5B%5B%22AKsRol9cyzk4CHOSNU_BD9u3AuLQLp6soCM2caAZ3Q5YkCV77JTcL7pp7nDVx1WImAKjL60TgjIj6vBdYgEFvcDucdzkCFfBtRx3AvxWoLp0a4cR1eQ66wV8SHTPpKVpHXaxdBZpVAjSIDkSLoVwriLy7U6tecCZDw%3D%3D%22%5D%5D",
            "_ga_TZN1DMYFRS": "GS1.1.1737341047.3.1.1737341294.32.0.0",
            "_chartbeat5": "777|5963|%2Fsearch%2F%25CE%2598%25CE%25B5%25CF%2581%25CE%25BC%25CF%258C%25CF%2582|https%3A%2F%2Fwww.tovima.gr%2Fsearch%2F%25CE%2598%25CE%25B5%25CF%2581%25CE%25BC%25CF%258C%25CF%2582%2Fpage%2F2%2F|B5tRIgDw_eZIrynCKCHow8kCUYFWy||c|DbYOp6DFOwBeD5FDkxv9ss6BJkWpQ|tovima.gr|"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")

        links = response.xpath(
            "//div[@class='simple-row ']/a/@href").extract()
        print(links)
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
            items.article_url = item
            # items.title = item.get("headline")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://www.tovima.gr/search/{current_keyword}/page/{current_page}/"
        yield feapder.Request(url, callback=self.parse_url, page=current_page, keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='single-article']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
