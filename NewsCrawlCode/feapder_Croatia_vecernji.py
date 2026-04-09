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

    country = 'Croatia'
    table = 'Croatia_vecernji'
    keywords =  [
    "Toplo", "Ekstremne topline", "Visoka temperatura", "Ekstremne temperature", "Topline događaji", "Povećanje temperature",
    "Utičaji temperature", "Visoka temperatura", "Intenzivno topline", "Uzlazanje temperature", "Topline događaj",
    "Uzlazanje temperature", "Intenzivni padovi kiše", "Intenzivna kiša", "Intenzivna kiša", "Ekstremni padovi kiše",
    "Suša", "Serijska suša", "Duga suša", "Voda nedostaje", "Prekid struje", "Prekid struje zbog temperature", "Prekid struje zbog topline",
    "Prekid struje zbog topline", "Vatrogas", "Vatrogas zbog temperature", "Vatrogas zbog topline", "Vatrogas zbog temperature",
    "Vatrogas zbog temperature", "Utičaji na poljoprivredu", "Topline događaj u poljoprivredi", "Štete na biljkama", "Poljoprivredna toplinska stresa",
    "Gubitak oksigena", "Topline udar", "Topline udar", "Gubitak oksigena zbog temperature", "Topline udar zbog temperature",
    "Utičaji na promet", "Promet zbog temperature", "Promet zbog topline", "Promet zbog temperature", "Ekološka katastrofa",
    "Ekološka katastrofa zbog temperature", "Utičaji temperature na okoliš", "Utičaji temperature na biotsku raznolikost",
    "Ekološka katastrofa zbog topline", "Zagađenje", "Zagađenje zbog temperature", "Zagađenje zbog topline", "Zagađenje zbog temperature",
    "Bijeljenje koralja", "Koralji zbog temperature", "Bijeljenje koralja zbog temperature"
]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://www.vecernji.ba/pretraga"
            params = {
                "query": f"{keyword}",
                "order": "-publish_from",
                "page": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.vecernji.ba/pretraga?order=-publish_from&query=temperature+",
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
            "csrftoken": "GeZJIV4OKkRvefjliQok4y08VTy6AD3r2yLWguxw0xdrEhTHfqaMogrs0bocQAtT",
            "_gid": "GA1.2.85757738.1737421573",
            "df_uid": "5cccd434-7175-4f39-acad-1467508713ce",
            "_pctx": "%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAEzIEYOBmADgDYOAJl5CArGI4AWAOy8AnGPnyQAXyA",
            "_pcid": "%7B%22browserId%22%3A%22m65ry1tt5rtkoy5x%22%7D",
            "__AP_SESSION__": "d4124874-c53f-42a5-b927-cb4907d0031e",
            "ezoab_646186": "mod140-c",
            "lp_646186": "https://www.vecernji.ba/",
            "ezovuuid_646186": "419383e2-e438-4601-5f37-3a6c86871d89",
            "ezoref_646186": "vecernji.hr",
            "_sotmsid": "0:m65ry23y:QwcZWSCj8HLBki2YYqAtFfxiRFQ5zqUC",
            "_sotmpid": "0:m65ry23y:myBFCZje5zLi532UjboBE0UbG5hmkhpM",
            "__pat": "3600000",
            "didomi_token": "eyJ1c2VyX2lkIjoiMTk0ODY2NTQtZmNlOC02NjA1LWJmNjYtZTIxNTczNzBkNzcwIiwiY3JlYXRlZCI6IjIwMjUtMDEtMjFUMDE6MDY6MTQuMDk0WiIsInVwZGF0ZWQiOiIyMDI1LTAxLTIxVDAxOjA2OjE4Ljc2N1oiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwiYW1hem9uIiwidHdpdHRlciJdfSwicHVycG9zZXMiOnsiZW5hYmxlZCI6WyJkZXZpY2VfY2hhcmFjdGVyaXN0aWNzIiwiZ2VvbG9jYXRpb25fZGF0YSJdfSwidmVuZG9yc19saSI6eyJlbmFibGVkIjpbImdvb2dsZSJdfSwidmVyc2lvbiI6MiwiYWMiOiJBRm1BQ0FGay5BRm1BQ0FGayJ9",
            "euconsent-v2": "CQLlOMAQLlOMAAHABBENBZFsAP_gAEPgAAAAKwtX_G__bWlr8X73aftkeY1P99h77sQxBhbJE-4FzLvW_JwXx2ExNA36tqIKmRIAu3TBIQNlHJDURVCgaogVryDMaEyUoTNKJ6BkiFMRI2dYCFxvm4tjeQCY5vr991dx2B-t7dr83dzyy4hHn3a5_2S0WJCdA5-tDfv9bROb-9IOd_x8v4v4_F7pE2_eT1l_tWvp7D9-cts_9XW99_fbff9Pn_-uB_-_X_vf_H36Cr4BJhoVEAZYEhIQaBhBAgBUFYQEUCAIAAEgaICAEwYFOwMAF1hIgBACgAGCAEAAIMgAQAAAQAIRABAAUCAACAQKAAMACAYCAAgYAAQAWAgEAAIDoGKYEEAgWACRmRUKYEIQCQQEtlQgkAQIK4QhFngEQCImCgAAAAAKQABAWCwOJJASoSCALiCaAAAgAQCCAAoQScmAAIAzZag8GTaMrTAMHzBIhpgGQBEEZCQaAAAA.f_wACHwAAAAA",
            "cX_P": "m65ry1tt5rtkoy5x",
            "__qca": "P0-1593916413-1737421578803",
            "pnespsdk_visitor": "m65ry1tt5rtkoy5x",
            "cX_G": "cx%3A1gr07o9ye0up5vdm7tlxyo2ef%3A214r34b3ki0mf",
            "_sharedID": "0a4dce53-b8ed-4fa3-9901-0e35cdea3f9f",
            "_sharedID_cst": "MCwMLKEsaw%3D%3D",
            "ezux_ifep_646186": "true",
            "sessionid": "j3c9m7nidx8wlgmt317df5qk41mt8cp4",
            "compass_uid": "59beedc6-ac87-4608-bc5c-0d1512aca7a6",
            "DotMetrics.DomainCookie": "{\"dc\":\"1e56ecea-5147-4587-b1a1-22bb93ef1a8f\",\"ts\":1737421707733}",
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "panoramaId_expiry": "1738026492909",
            "panoramaId": "c2ba4404dd1662779c299379ee5816d53938f08b6896985ba839d86735aa250d",
            "panoramaIdType": "panoIndiv",
            "___nrbi": "%7B%22firstVisit%22%3A1737421574%2C%22userId%22%3A%2259beedc6-ac87-4608-bc5c-0d1512aca7a6%22%2C%22userVars%22%3A%5B%5D%2C%22futurePreviousVisit%22%3A1737421708%2C%22timesVisited%22%3A2%7D",
            "DM_SitId218": "1",
            "DM_SitId218SecId4444": "1",
            "ezppid_ck": "b49fce43663fdd0b9347ba0e1f097160",
            "__gads": "ID=1a4f1894cf242231:T=1737421709:RT=1737421709:S=ALNI_MbDJxSS-XvsbcPu6xiVXH7UkwIPew",
            "__gpi": "UID=00000ff394ef2e79:T=1737421709:RT=1737421709:S=ALNI_MZ4vFx9DVZbWuZaW81wXRNH6ZpYPg",
            "__eoi": "ID=9fc3980beef3654f:T=1737421709:RT=1737421709:S=AA-AfjYk1ulrFz3FrZpn-tXThDvY",
            "ezhbf": "0",
            "_sharedid": "23af94ef-8390-4be5-97bd-338d02285639",
            "_sharedid_cst": "MCwMLKEsaw%3D%3D",
            "_lr_retry_request": "true",
            "_lr_env_src_ats": "false",
            "pbjs-unifiedid": "%7B%22TDID%22%3A%221bf001f8-5fe3-426f-8626-c82066db4b6d%22%2C%22TDID_LOOKUP%22%3A%22FALSE%22%2C%22TDID_CREATED_AT%22%3A%222025-01-21T01%3A09%3A30%22%7D",
            "pbjs-unifiedid_cst": "MCwMLKEsaw%3D%3D",
            "_lr_sampling_rate": "100",
            "_ga_VBH636XDF0": "GS1.2.1737421573.1.1.1737421824.0.0.0",
            "__pvi": "eyJpZCI6InYtbTY1czB4eXprdDV3Yzd1ZyIsImRvbWFpbiI6Ii52ZWNlcm5qaS5iYSIsInRpbWUiOjE3Mzc0MjE4MjUyNDd9",
            "ezovuuidtime_646186": "1737421810",
            "active_template::646186": "pub_site.1737421810",
            "ezopvc_646186": "6",
            "___nrbic": "%7B%22sessionId%22%3A%224405b59b-3bee-4b2f-800d-a4f550998a43%22%2C%22currentVisitStarted%22%3A1737421574%2C%22sessionVars%22%3A%5B%5D%2C%22previousVisit%22%3A1737421574%2C%22visitedInThisSession%22%3Atrue%2C%22pagesViewed%22%3A4%2C%22landingPage%22%3A%22https%3A//www.vecernji.ba/%22%2C%22referrer%22%3A%22https%3A//www.vecernji.hr/%22%2C%22lpti%22%3Anull%7D",
            "pnespsdk_ssn": "%7B%22%24s%22%3A1737421579817%2C%22visitNumber%22%3A5%7D",
            "__tbc": "%7Bkpex%7DxtTda5IuzXp7F9XrBsMDUtaDQOSvP9bnHRkfugE2Z0TIUm2vVG6e-JT7GFltNnfB",
            "xbc": "%7Bkpex%7D7onuFUIpzfif9iD5YXjEig",
            "_ga": "GA1.2.1078851865.1737421573",
            "ezux_lpl_646186": "1737421828670|5cc1d2e7-25a1-42d1-42fe-c3cb03b00814|false",
            "_ga_2S79X5VN7G": "GS1.1.1737421573.1.1.1737421828.13.0.526228446",
            "_ga_K97SE6L60V": "GS1.1.1737421572.1.1.1737421828.0.0.0",
            "cto_bundle": "9VQejV9PT1JQUkJIcFlMNFJhcloxd0pnNGdlbyUyRjdkNW5IV25vTjdDd3VnS0xDMVRYdVFlWURXVFZPc015VzZhUnc3SUI1ZmZ4b0tEaUQ3RDAwd2ttd3hOUmFJJTJCaElMdnZ6d043OVZsWUJmNWRBQnUzb0pCcyUyRmxoNTRiQkxtbWwzek1YaUtXRG9ib0VhTzI0YTdvTndRQ3B6NFElM0QlM0Q",
            "cto_bidid": "9DmWIl90Tlg3MHBIU3ZzWTdEcDMzSzh2bG1admxTSkpnc3BEMFRpM2IwbmlrRUs2cHVRYURDb2FPTDNmQjk4bUhYbzE1b3JheUY5ZTBZV24lMkZVazF2a0lBRWJndHNXU2h2ekFwcjhGSkdEWFY3UlM4JTNE",
            "_tfpvi": "ZWI5M2M0NmMtNzIyYi00OGNlLWI2MTgtYmUyZDg3YjE1ZmMzIzAtMw%3D%3D",
            "cto_dna_bundle": "burJmV9PT1JQUkJIcFlMNFJhcloxd0pnNGdSZjV4Y3BGUGg5dk5mZU01djluS0glMkJKUzQ4QmYzM2xMTm9rVk9LbEVQaHNxdUluVyUyQmFrMSUyQnhnZVpmQ2hZMzJDdyUzRCUzRA",
            "ezux_et_646186": "91",
            "ezux_tos_646186": "264"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//article/a/@href").extract()
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
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.vecernji.ba/pretraga"
        params = {
            "query": f"{current_keyword}",
            "order": "-publish_from",
            "page": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@id='js_content']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@itemprop='datePublished']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
