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

    country = 'Italy'
    table = 'Italy_ilsole24ore'
    keywords = [
        "calore",
        "onda di calore estrema",
        "temperature elevate",
        "temperature estreme",
        "eventi di onda di calore",
        "aumento di temperature elevate",
        "effetti del calore",
        "temperature elevate",
        "forte calore",
        "aumento di temperatura",
        "evento di calore",
        "aumento di temperatura",
        "pioggia intensa",
        "precipitazioni intense",
        "pioggia torrenziale",
        "pioggia estrema",
        "siccità",
        "siccità grave",
        "siccità prolungata",
        "scarsità di acqua",
        "blackout",
        "blackout per calore",
        "blackout per onda di calore",
        "blackout causato dal calore",
        "incendio",
        "incendio per calore",
        "incendio da calore",
        "incendio da temperatura",
        "incendio causato dal calore",
        "effetti sull'agricoltura",
        "onda di calore sull'agricoltura",
        "danno alle colture",
        "stress termico sull'agricoltura",
        "ipossia",
        "colpo di calore",
        "ipossia da calore",
        "colpo di calore da temperatura",
        "effetti sul traffico",
        "traffico per calore",
        "traffico per onda di calore",
        "traffico da temperatura",
        "disastro ecologico",
        "disastro da calore",
        "ambiente caldo",
        "effetti del calore sulla biodiversità",
        "onda di calore ecologica",
        "inquinamento",
        "inquinamento da calore",
        "inquinamento termico",
        "inquinamento da temperatura",
        "bleaching delle coralline",
        "corallo da calore",
        "corallo da temperatura",
        "bleaching da temperatura"
    ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.ricerca24.ilsole24ore.com/api/search"
            data = {
                "keyword": f"{keyword}",
                "orderBy": "1",
                "pageNumber": "1",
                "pageSize": "10",
                "fromDate": "",
                "toDate": "",
                "filter": ""
            }
            yield feapder.Request(url, data=data, callback=self.parse_url, page=page, method='POST',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://www.ricerca24.ilsole24ore.com",
            "priority": "u=1, i",
            "referer": "https://www.ricerca24.ilsole24ore.com/?cmd=static&chId=30&path=/search/search_engine.jsp&field=Titolo|Testo&orderBy=score+desc&chId=30&disable_user_rqq=false&keyWords=calore&pageNumber=4&pageSize=10&fromDate=&toDate=&filter=all",
            "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Microsoft Edge\";v=\"133\", \"Chromium\";v=\"133\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
            "x-requested-with": "XMLHttpRequest"
        }
        request.cookies = {
            "OptanonAlertBoxClosed": "2025-02-15T01:02:47.486Z",
            "eupubconsent-v2": "CQM2hUAQM2hUAAcABBITBdF8AP_gAEPgAChQKhNV_G__bWlr8X73aftkeY1P99h77sQxBhbJE-4FzLvW_JwXx2ExNAz6tqIKmRIAu3TBIQNlGJDURVCgaogVrSDMaEiUoTNKJ6BkiFMRI2dYCFxvm4tjeQCY5vr991dx2B-t7dr83dzyy4BHn3a5_2S1WJCdAYetDfv9bROb-9IOd_x8v4v4_F7pE2_eT1l_tWvp7D9-cts_9XW99_fbff9Pn_-uF_-_X_sFQACTDQqIAyyJCQg0DCCBACoKwgIoEAQAAJA0QEAJgwKcgYALrCRACAFAAMEAIAAQZAAgAAEgAQiACAAoEAAEAgUAAQAEAwEADAwABgAsBAIAAQHQMUwIIBAsAEjMigUwIQAEggJbKhBIAgQVwhCLPAIAERMFAAACAAUgACAsFgcSSAlYkEAXEE0AABAAgEEABQik7MAQQBmy1F4Mm0ZWmBYPmC55TAMgCIAA.f_wACHwAAAAA",
            "_OptanonConsentOld": "isGpcEnabled=0&datestamp=Sat+Feb+15+2025+09%3A02%3A47+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=69b7168a-661e-464d-88a4-6285518b69e0&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=3%3A1%2C4%3A1%2C2%3A1%2C1%3A1%2CV2STACK42%3A1&intType=1",
            "SoleCookieAccept": "Y",
            "_dfduuid": "61498b5f-729f-46fd-965c-44776171d0fb",
            "_dfpid": "",
            "_ga": "GA1.2.986243307.1739581548",
            "_gid": "GA1.2.674842013.1739581548",
            "wt_session_s24": "1739581548158",
            "s_fid": "62D8FA872AC4C747-2A6E76874515551A",
            "ev7_NW": "D%3Dc7",
            "s_cc": "true",
            "__gads": "ID=3a78a5198a8a0676:T=1739581341:RT=1739582351:S=ALNI_MZxt74vTpT1C-tE6IBGjLePyXPWCA",
            "__gpi": "UID=0000103a151bd81b:T=1739581341:RT=1739582351:S=ALNI_MaFmtDlxJo9xw1v-H-UC4ggpzStWg",
            "__eoi": "ID=08c720a5be8612e2:T=1739581341:RT=1739582351:S=AA-Afja0u4WeJNdOPehN70x9JBCf",
            "OptanonConsent": "isGpcEnabled=0&datestamp=Sat+Feb+15+2025+09%3A19%3A42+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=69b7168a-661e-464d-88a4-6285518b69e0&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=3%3A1%2C4%3A1%2C2%3A1%2C1%3A1%2CV2STACK42%3A1&intType=1&geolocation=CN%3BSC&AwaitingReconsent=false",
            "cto_bundle": "44072F9PT1JQUkJIcFlMNFJhcloxd0pnNGdiR24zWEdQUCUyQm5lTllkaDhOdU9qTUhYWmx5eDUlMkZuSFlVbUhWeERPMUlmWU9TVFdZUUIzeXROdGlsNSUyRjRUaTBRQzdkOVUyYXRhZCUyQjVXaG9JSkZIeUxQb3olMkZ0ekhZcDhFVU42YkxZS2JoZ01VbEZCRWlJSmhpclFBYTBDdzBNOE1RJTNEJTNE",
            "blaize_session": "c394c9af-9707-40bc-afee-005ad57c7836",
            "blaize_tracking_id": "19b35051-f72a-4334-a3be-f1f7da0fc42a",
            "_k5a": "61@{\"u\":[{\"uid\":\"tKQbQd0oUPKue2rW\",\"ts\":1739582387},1739672387]}",
            "_fbp": "fb.1.1739582390249.626511021749302830",
            "zit.data.toexclude": "0",
            "_sxh": "1567,",
            "___nrbi": "%7B%22firstVisit%22%3A1739582628%2C%22userId%22%3A%222f5368d1-e5d8-41f7-9d2b-e7eaa2d737f0%22%2C%22userVars%22%3A%5B%5D%2C%22futurePreviousVisit%22%3A1739582628%2C%22timesVisited%22%3A1%7D",
            "compass_uid": "2f5368d1-e5d8-41f7-9d2b-e7eaa2d737f0",
            "wt_gdpr_988195797407130": "4173958152400460353",
            "wt_cookiecontrol": "1",
            "_sanba": "0",
            "_sxo": "{\"R\":0,\"tP\":0,\"tM\":0,\"sP\":1,\"sM\":0,\"dP\":1,\"dM\":0,\"dS\":1,\"tS\":0,\"cPs\":1,\"lPs\":[0],\"sSr\":0,\"sWids\":[],\"wN\":0,\"cdT\":0,\"F\":1,\"RF\":1,\"w\":0,\"SFreq\":1,\"last_wid\":0,\"bid\":1068,\"accNo\":\"\",\"clientId\":\"\",\"isEmailAud\":0,\"isPanelAud\":0,\"hDW\":0,\"isRegAud\":0,\"isExAud\":0,\"isDropoff\":0,\"devT\":0,\"exPW\":0,\"Nba\":-1,\"userName\":\"\",\"dataLayer\":\"\",\"localSt\":\"\",\"emailId\":\"\",\"emailTag\":\"\",\"subTag\":\"\",\"lVd\":\"2025-2-15\",\"oS\":\"19b35051-f72a-4334-a3be-f1f7da0fc42a\",\"cPu\":\"https://www.ilsole24ore.com/art/pompe-calore-polo-soddisfare-richiesta-AERUyj4B\",\"pspv\":0,\"pslv\":0,\"pssSr\":0,\"pswN\":0,\"psdS\":0,\"pscdT\":0,\"RP\":0,\"TPrice\":0,\"ML\":\"\",\"isReCaptchaOn\":false,\"reCaptchaSiteKey\":\"\",\"reCaptchaSecretKey\":\"\",\"extRefer\":\"\",\"dM2\":0,\"tM2\":0,\"sM2\":0,\"RA\":0,\"ToBlock\":-1,\"CC\":null,\"groupName\":null}",
            "adms_s": "Less%20than%201%20day",
            "s_pv": "N24%3Aricerca24",
            "___nrbic": "%7B%22isNewUser%22%3Atrue%2C%22previousVisit%22%3A1739582628%2C%22currentVisitStarted%22%3A1739582628%2C%22sessionId%22%3A%227c14d211-7d3d-4a47-a88c-02097091aba8%22%2C%22sessionVars%22%3A%5B%5D%2C%22visitedInThisSession%22%3Atrue%2C%22pagesViewed%22%3A11%2C%22landingPage%22%3A%22https%3A//www.ilsole24ore.com/art/pompe-calore-polo-soddisfare-richiesta-AERUyj4B%3Frefresh_ce%26nof%22%2C%22referrer%22%3A%22%22%2C%22lpti%22%3A%222022-10-21T16%3A03%3A00+0200%22%7D",
            "utag_main": "v_id:01950723581c0090ae334d5760f00507d002907500bd0$_sn:1$_se:23$_ss:0$_st:1739586463260$ses_id:1739581511709%3Bexp-session$_pn:2%3Bexp-session",
            "wt_rla": "988195797407130%2C14%2C1739583444456",
            "s_nr": "1739584762745-Repeat",
            "adms": "1739584762745"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json['search'].get("result").get("documents")

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
            items.article_url = "https://www.ilsole24ore.com" + item.get("fields").get("URL")
            items.title = item.get("fields").get("Titolo")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.ricerca24.ilsole24ore.com/api/search"
        data = {
            "keyword": f"{current_keyword}",
            "orderBy": "1",
            "pageNumber": f"{current_page}",
            "pageSize": "10",
            "fromDate": "",
            "toDate": "",
            "filter": ""
        }
        yield feapder.Request(url, data=data, callback=self.parse_url, page=current_page, method='POST',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        # items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(
            response.xpath("//p[@class='atext']//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
