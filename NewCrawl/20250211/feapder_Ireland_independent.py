# -*- coding: utf-8 -*-
"""

集群运行

"""
import base64
import re

import feapder
from feapder import Item
import json
from lxml import etree


class AirSpiderDemo(feapder.AirSpider):
    def __init__(self):
        super().__init__()
        self.previous_links = None  # 初始化 previous_links 为 None

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

    country = 'Ireland'
    table = 'Ireland_independent'
    keywords =  ["heatwave","Extreme heatwave", "High temperature", "Extreme temperature", "Heatwave event", "Increase in high temperature", "High temperature impact", "Strong heat", "Temperature rise", "Heat event", "Temperature increase", "Heavy rainfall", "Heavy precipitation", "Torrential rain", "Extreme rainfall", "Drought", "Severe drought", "Long-term drought", "Water shortage", "Power outage", "High temperature power outage", "Heatwave power outage", "Power outage caused by high temperature", "Fire", "High temperature fire", "Heat fire", "Temperature fire", "Heat-induced fire", "Agricultural impact", "Heatwave agriculture", "Crop damage", "Agricultural heat stress", "Hypoxia", "Heatstroke", "Heat-induced heatstroke", "High temperature hypoxia", "High temperature heatstroke", "Traffic impact", "High temperature traffic", "Heatwave traffic", "Temperature traffic", "Ecological disaster", "Heat disaster", "High temperature environment", "Impact of heat on biodiversity", "Heatwave ecology", "Pollution", "High temperature pollution", "Heat pollution", "Temperature pollution", "Coral bleaching", "High temperature coral reefs", "Temperature coral bleaching" ]
    def start_requests(self):

        for keyword in self.keywords:
            page = 1
            url = "https://www.independent.ie/graphql-blue-mhie"
            params = {
                "variables": "{\"brand\":\"independent.ie\",\"count\":10,\"search\":\"heavy rain\",\"publishedAfter\":\"\",\"publishedBefore\":\"\",\"sections\":[],\"ordering\":\"MOST_RECENT\",\"sourcesetCroppingInput\":{\"resizeMode\":\"CENTER_CROP\",\"cropsMode\":\"LANDSCAPE\",\"fallbackResizeMode\":\"SMART_CROP\",\"sizes\":[{\"key\":\"xsmall\",\"width\":120,\"height\":80},{\"key\":\"small\",\"width\":160,\"height\":107},{\"key\":\"smallMobile\",\"width\":240,\"height\":160},{\"key\":\"medium\",\"width\":320,\"height\":213},{\"key\":\"large\",\"width\":640,\"height\":427},{\"key\":\"xlarge\",\"width\":960,\"height\":640},{\"key\":\"xxlarge\",\"width\":1280,\"height\":853}]},\"after\":\"\"}",
                "operationName": "webv2_SearchArticlesConnection_indo_3_2",
                "persisted": "true",
                "extensions": "{\"persistedQuery\":{\"version\":1,\"sha256Hash\":\"43e500c22cefdeeb9dd4d386a63bcff60485f16d48e6419d65365f8e77e844b5\"}}"
            }
            variables = json.loads(params['variables'])

            # 更新 search 和 after 字段
            variables['search'] = f"{keyword}"
            variables['after'] = None  # 或根据需要设置为 None
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, method='GET',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "apollographql-client-name": "article-list-v2",
            "apollographql-client-version": "0.0.1246-indo",
            "priority": "u=1, i",
            "referer": "https://www.independent.ie/search?keyword=heavy+rain&daterange=all&datestart=&dateend=",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0",
            "x-client-name": "article-list-v2-indo"
        }
        request.cookies = {
            "_segmentgroup": "D",
            "gig_bootstrap_4_CO8231Ix1RbYi2EmMbsrlw": "gigya-cp_ver4",
            "didomi_token": "eyJ1c2VyX2lkIjoiMTk0ZjQyYTgtOWNlOC02OTkwLWIzODYtNjExOTlmZDBmODcyIiwiY3JlYXRlZCI6IjIwMjUtMDItMTFUMDg6NDA6MTYuMDc4WiIsInVwZGF0ZWQiOiIyMDI1LTAyLTExVDA4OjQwOjIwLjQ2MVoiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwiYzphZ25vcGxheS1NelljRjRIRiIsImM6cHVic3RhY2staVloZlEzaFEiLCJjOnlvdXR1YmUiLCJjOmhvdGphciIsImM6b21uaXR1cmUtYWRvYmUtYW5hbHl0aWNzIiwiYzplYmF5LXBhcnRuZXItbmV0d29yayIsImM6aW50ZWxsaWFkIiwiYzpiYXRjaC1qWFB4TU1MTiIsImM6bGlua2VkaW4iLCJjOmZhY2Vib29rcC1VRFU4WUtOZiIsImM6Z2V0c2l0ZWNvbi05Q3F6RzdaNiIsImM6aW90ZWNobm9sLXlZcGNmdGR6IiwiYzp0d2l0dGVyLTN3MzM5Y0w2IiwiYzppbmZlY3Rpb3VzLW1lZGlhIiwiYzp0dXJibyIsImM6YW5hbGlnaHRzLTdteUVIM2VrIiwiYzpnb29nbGVhbmEtNFRYbkppZ1IiLCJjOnNvY2lvbWFudGktbU1URzh4ZzQiLCJjOm5leHRyb2xsLUdEbnBBREdiIiwiYzp6YWxhbmRvLXBUS1lWTWFiIiwiYzp2aXJ0dWFsbW4tRU1Bek1MRFciLCJjOnBtZy1aRTJDeUNGayIsImM6b21uaWNvbW1lLXlQaWo3Z1laIiwiYzppbnRlcnB1YmxpLThIWjhQZkczIiwiYzpnc2tpbm5lci1uVUUzNFAySCIsImM6ZG1haW5zdGl0LWZ6UTh5QWlyIiwiYzpiZXRnZW5pdXMtQnRoNmo4UkQiLCJjOmJsdWVjb25pYy1tZmNlUFVaOSIsImM6aW5zdGFncmFtLWtSRnAzeUpoIiwiYzpmYWNlYm9vay1WQmk0VkJaQyIsImM6YWZmaWxpbmV0IiwiYzpjZW50cm8taVVXVm1ONE4iLCJjOmZyYWN0aW9uYWwtbjJqWURGUE4iLCJjOmlnbml0aW9uby1MVkFNWmRuaiIsImM6Z29vZ2xlYW5hLUFlUGZBS2hKIl19LCJwdXJwb3NlcyI6eyJlbmFibGVkIjpbInNvY2lhbF9tZWRpYSJdfSwidmVuZG9yc19saSI6eyJlbmFibGVkIjpbImM6dHVyYm8iXX0sInZlcnNpb24iOjIsImFjIjoiQzJhQkVBRVlBTElBWFFBMkFCNkFFcUFNUUFtNEI1b0QzQVBlQWo0QjZvRUd3SWtnUzFBbjRCUW9DaW9GaHdMVWdZaUF4dUJuTURYSUhUZ09yQWRoQkI2Q0lFRVpvSjNnVUVBb1BCYk0uQUFBQSJ9",
            "euconsent-v2": "CQMqb4AQMqb4AAHABBENBcFgAP_gAELgAAAAJVhD7CbEYWFCwG51YLsAOAhSZMAYAsQAAAaAE2ABgBKUIAQCgkAYEACgBAACAAAAACQBIAJAGABAAUAAQAAAAAAAQEgAAABIIAAAgAAAAAAIAAACAIAAQAAIAEAAEAAAmQgAAIAAGEAAgAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAEAABAIAAAAAAAAAIFWQBgBgADuAI4Ag8BTYCrABSUAGAAIKzlIAMAAQVnHQAYAAgrOQgAwABBWcJABgACCs4AA.f_wACFwAAAAA",
            "_gcl_au": "1.1.413913571.1739263221",
            "_vwo_uuid_v2": "DB950033E1539B9E288C95F1961D7A08A|643df2a99814b9df160d4981270a31e1",
            "_fbp": "fb.1.1739263221600.400202173229304617",
            "_tt_enable_cookie": "1",
            "_ttp": "jNTbOu_IXhdS1Lwd_DwH0SDEVpU.tt.1",
            "_mhtc_cId": "752105a6-e5d5-4d76-ba1c-bc3e9c75b8f4",
            "sessionId": "bd3ce3d5-7c7b-4739-ab89-a146ee78949b",
            "_mhtrdisableshortname": "1",
            "_ga": "GA1.1.1447938092.1739263222",
            "_mhtc_sId": "bd3ce3d5-7c7b-4739-ab89-a146ee78949b",
            "_sotmsid": "0:m708f0do:uOvQ9PI7rxNK1j6_jBcA8embrjURUnJt",
            "_sotmpid": "0:m708f0do:rscBAOWhj65m2~bhh_pZY0gj2F6bcD9e",
            "sc": "e1c17fe1-07cc-4827-9466-a4ffa168a8c9.2",
            "viewSq": "2",
            "_ga_W2EK8THFQJ": "GS1.1.1739263222.1.1.1739263232.0.0.0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        # print(response.json)
        next_page_info = response.json["data"]["search"].get("pageInfo").get("endCursor")

        links = response.json['data']['search'].get("edges")
        # print(links)
        current_page = request.page

        current_links = next_page_info
        if self.previous_links is not None and current_links == self.previous_links:
            print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理

        request.previous_links = current_links  # 更新上一页的链接列表

        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            items = Item()
            items.table_name = self.table  # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url = "https://www.independent.ie" + item.get("node").get("relativeUrl")
            items.title = item.get("node").get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.independent.ie/graphql-blue-mhie"
        params = {
            "variables": "{\"brand\":\"independent.ie\",\"count\":10,\"search\":\"heavy rain\",\"publishedAfter\":\"\",\"publishedBefore\":\"\",\"sections\":[],\"ordering\":\"MOST_RECENT\",\"sourcesetCroppingInput\":{\"resizeMode\":\"CENTER_CROP\",\"cropsMode\":\"LANDSCAPE\",\"fallbackResizeMode\":\"SMART_CROP\",\"sizes\":[{\"key\":\"xsmall\",\"width\":120,\"height\":80},{\"key\":\"small\",\"width\":160,\"height\":107},{\"key\":\"smallMobile\",\"width\":240,\"height\":160},{\"key\":\"medium\",\"width\":320,\"height\":213},{\"key\":\"large\",\"width\":640,\"height\":427},{\"key\":\"xlarge\",\"width\":960,\"height\":640},{\"key\":\"xxlarge\",\"width\":1280,\"height\":853}]},\"after\":\"YnlwYXNzX3BheXdhbGw9MSZvcmRlcmluZz0tcHVibGlzaGVkX2F0JnBhZ2U9MSZwYWdlX3NpemU9MTAmcHJvZHVjdHM9aW5kZXBlbmRlbnQuaWUmcT1oZWF2eStyYWluJnNlYXJjaF9hZnRlciU1QiU1RD0xNzM4MjMxMjAwMDAwJnNlYXJjaF9hZnRlciU1QiU1RD0xNTgxNTI5NzQ=\"}",
            "operationName": "webv2_SearchArticlesConnection_indo_3_2",
            "persisted": "true",
            "extensions": "{\"persistedQuery\":{\"version\":1,\"sha256Hash\":\"43e500c22cefdeeb9dd4d386a63bcff60485f16d48e6419d65365f8e77e844b5\"}}"
        }
        variables = json.loads(params['variables'])

        # 更新 search 和 after 字段
        variables['search'] = f"{current_keyword}"
        variables['after'] = next_page_info  # 或根据需要设置为 None
        print(params, "-" * 10)
        params['variables'] = json.dumps(variables)
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, method='GET',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        # items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@data-testid='article-body']//p/text()").extract())
        items.author = ''
        items.pubtime = ''
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
