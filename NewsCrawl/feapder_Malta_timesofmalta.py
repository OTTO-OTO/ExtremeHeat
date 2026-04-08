import json

import feapder
from feapder import Item
from lxml import etree
import re


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[5, 8],
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
    country = 'Malta'
    table = 'Malta_timesofmalta'
    keywords = ['heatwave',
                # 'Heat', 'High Temperature', 'Heavy Rain', 'Drought', 'Power Outage from Heat', 'Fire',
                # 'Air Pollution', 'Climate Change', 'Crop Yield Reduction', 'Oxygen Deficiency',
                # 'High Temperature Affecting Traffic', 'Ecological Disaster', 'Climate Change Affecting Economy',
                # 'Marine Heatwave', 'High Temperature Pollution', 'Coral'
                ]

    previous_links = None
    def start_requests(self):
        for keyword in self.keywords:
            page = 55
            url = "https://www.timesofmalta.com/search"
            params = {
                "keywords": f"{keyword}",
                "include%5B0%5D": "title",
                "include%5B1%5D": "body",
                "tags%5B0%5D": "49",
                "tags%5B1%5D": "712",
                "tags%5B2%5D": "310",
                "range": "all_times",
                "from": "2000-01-01",
                "until": "2025-01-06",
                "sort": "score",
                "order": "desc",
                "author": "0",
                "page": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.timesofmalta.com/search?keywords=High+Temperature&include%5B%5D=title&include%5B%5D=body&tags%5B%5D=49&tags%5B%5D=712&tags%5B%5D=310&range=all_times&from=2000-01-01&until=2025-01-06&sort=score&order=desc&author=0",
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
            "_gid": "GA1.2.729706351.1736132785",
            "_fbp": "fb.1.1736132785650.853611703613843806",
            "_pctx": "%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAEzIFYAODgNgCYAzABYAjF0EiADAE4OM0YP4gAvkA",
            "_pcid": "%7B%22browserId%22%3A%22m5kgmv4rcses0yba%22%7D",
            "__pnahc": "0",
            "fpestid": "5xt2rKD6bp4o-MbkZ-04KQSDBR2B_BaIG75nEBL2KAHFFOZq18dkEKizaqC0UAF9c33-lg",
            "___nrbi": "%7B%22firstVisit%22%3A1736132786%2C%22userId%22%3A%22318813a6-6480-46e9-8fac-e8690f5db487%22%2C%22userVars%22%3A%5B%5D%2C%22futurePreviousVisit%22%3A1736132786%2C%22timesVisited%22%3A1%7D",
            "compass_uid": "318813a6-6480-46e9-8fac-e8690f5db487",
            "__pat": "3600000",
            "xbc": "%7Bkpcd%7DChBtNWtnbXY0cmNzZXMweWJhEgo0Mm9BNU16RXBlGjxWVEUwYWJkZElWOVdOc2dvVmNFSWNnM0hweGdrZVd3ajMzMHRnR3pHeWRmV0NURk9BOWd2U25ORFVwbEwgAA",
            "cX_P": "m5kgmv4rcses0yba",
            "cX_G": "cx%3A1o6y0nguxsrxp1rb37hfp1mi55%3A1t574vf2ordqs",
            "FCCDCF": "%5Bnull%2Cnull%2Cnull%2C%5B%22CQKzyIAQKzyIAEsACBENBXFoAP_gAEPgABiYINJD7C7FbSFCwH53aLsEMAhHRtAAQoQgAASBAmABQAKQIBQCgkAQFAygBCACAAAAICRBIQIECAAAAUAAQAAAAAAEAAAAAAAIIAAAgAEAAAAIAAACAIAAEAAIAAAAEAAAmAgAAIIACAAAgAAAAAAAAAAAAAAAAgCIAAAAAAEAAAAAAAAAAQPaSD2F2K2kKFkPCuwXYIYBCujaAAhQhAAAkCBMACgAUgQAgFJIAgCJFAAEAAAAAAQEiCQAAQABAAAIACgAAAAAAIAAAAAAAQQAABAAIAAAAAAAAEAQAAIAAQAAAAIAABEhCAAQQAEAAAAAAAQAAAAAAAAAAABAAAAAAAAAIAA.f_gAAAAAAAA%22%2C%222~70.89.93.108.122.149.196.236.259.311.313.323.358.385.415.442.449.486.494.495.540.574.609.864.981.1029.1048.1051.1067.1095.1097.1126.1205.1276.1301.1329.1365.1415.1449.1514.1516.1570.1577.1598.1616.1651.1716.1735.1753.1765.1782.1870.1878.1889.1917.1958.1960.1985.2072.2253.2299.2328.2331.2373.2415.2506.2526.2531.2567.2568.2571.2572.2575.2624.2677.2778.2878.2898.3234~dv.%22%2C%226E484C93-69A1-4958-A150-F4FC20C2D041%22%5D%5D",
            "_oid": "f9c53528-4ad0-4fcc-a2a0-5c3f576c7fe6",
            "panoramaId_expiry": "1736737751879",
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "panoramaId": "0010a56387b454fb99733e574a5e16d539386232f10af484e6cddb3c256de10d",
            "XSRF-TOKEN": "eyJpdiI6IlhQbnNITGRCQVkrTWM2MklmZWMwVGc9PSIsInZhbHVlIjoiT2ZGMDdYS3VtbnlkbWlBYURSZzNWVTF0eC94TFcwZjU5YWF4T0h0dUIxYktHaWhVdFo5M2Q4QkJHUjZaQnlqRythTmxiaDlVVFRCdnNINDhlRnJ6OTZuaHZpQUFvYzRSTmFNSmpadTlYcytZeE13TnlOTGRSdlhEY1RGOEJwQTAiLCJtYWMiOiI0NTdmYzIxZTgwNDVhYTQyNjZhZTIyYjVhZWFlOThkY2Y1ZDdjMTgyMDAyNjZkOWI4ZmY0NzZiOWZiZDFhODZkIiwidGFnIjoiIn0%3D",
            "tomweb_session": "eyJpdiI6Ilg3OHF5OUdEeUtqWUVQTFBlMll5K1E9PSIsInZhbHVlIjoiVmp2M0xGTFNvKzRJbXp0bXlnVWVGbWJCeFhEbFBUTjBMeGhCUzNUWE5lSzc0cnBCbVlidTI5YjVqMWVpM09VTGxnSkRpTVpRdDl1M2N6cllDU2JKOGVtdy81c1A1clp6N0tMNkdhWThxNjhiOFU3dU04eWEyT25tOWVMeWlGTUciLCJtYWMiOiJhMTQ5MzQ5MmRmOWQyNjZkNTExYzg3MDJjYzFjNGUxNmE3NGMzNDFhYWU0YWFmMjQ0ZjAzZmJhMWU2ZmYxOTE3IiwidGFnIjoiIn0%3D",
            "_ga": "GA1.2.1661158512.1736132785",
            "_gat_UA-18463442-2": "1",
            "_ga_QKDS2VG3HT": "GS1.1.1736132784.1.1.1736133402.60.0.0",
            "___nrbic": "%7B%22isNewUser%22%3Atrue%2C%22previousVisit%22%3A1736132786%2C%22currentVisitStarted%22%3A1736132786%2C%22sessionId%22%3A%225dd49877-023b-4c1b-89fe-c14ef500cad8%22%2C%22sessionVars%22%3A%5B%5D%2C%22visitedInThisSession%22%3Atrue%2C%22pagesViewed%22%3A9%2C%22landingPage%22%3A%22https%3A//www.timesofmalta.com/%22%2C%22referrer%22%3A%22%22%2C%22lpti%22%3Anull%7D",
            "__pvi": "eyJpZCI6InYtbTVrZ212NTNxcWVmMm5xZSIsImRvbWFpbiI6Ii50aW1lc29mbWFsdGEuY29tIiwidGltZSI6MTczNjEzMzQwMzAxMX0%3D",
            "__adblocker": "false",
            "__tbc": "%7Bkpcd%7DChBtNWtnbXY0cmNzZXMweWJhEgo0Mm9BNU16RXBlGjxWVEUwYWJkZElWOVdOc2dvVmNFSWNnM0hweGdrZVd3ajMzMHRnR3pHeWRmV0NURk9BOWd2U25ORFVwbEwgAA",
            "cto_bundle": "cKbLd19PT1JQUkJIcFlMNFJhcloxd0pnNGdaMmM3bjVBRUElMkJqdWpQJTJGOW1pY09NcmJGSG5MOGJ0OEthR25qSHg5bHRuNmVUcGtBRlUwJTJGdWZaV3pLaVFQV1YlMkJ0ZTJXQWY0ZmdsajdscmNJN2RzMERsOGV1MzVyQ1hkTFNldWozc3lBb3BGcENBSlpJQjJSWjNwb2VGRFNVOWlmUSUzRCUzRA",
            "cto_bidid": "weqreF83OGZ4bEoxVFJrSXl4YnJvUkhWSFdyOW9DMUwlMkJrRFluaHVCd3RlZG51bU44MGZLTjh1aXBzbUlia0lHWkpGT1hTRHZSNEJ1T1VmTFNXb2JJSVBBTFdzM3Bma2dudG52MURjUWhqVE41cUtGZUxkQ012VGxUSiUyQmc0TFFmYjNsZnA",
            "cto_dna_bundle": "Sq-eP19PT1JQUkJIcFlMNFJhcloxd0pnNGdSZjV4Y3BGUGg5dk5mZU01djluS0glMkJKUzQ4QmYzM2xMTm9rVk9LbEVQaHMlMkZvNUFudURSelZnJTJCa0xjcHIzSjlKUSUzRCUzRA",
            "FCNEC": "%5B%5B%22AKsRol8e2SNIRt0LGjr3NWW-Yvgy5E-TcKmiq4sxJzzcDVhH9zT-Ak5DNgxpsyylWJ_vSqhUXvv256La7NqqOl9HmhwX_6m5eIVl8b0J82U01PGYUP1efH6MnfX9KC-KqPUPdr5e7qZOpUwU5rrUyCg3adKBVbEXgg%3D%3D%22%5D%5D"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        pattern = r'<script type="application/ld\+json" id="listing-ld">(.*?)</script>'

        # 使用re.DOTALL让.匹配包括换行符在内的所有字符
        matches = re.findall(pattern, response.text, re.DOTALL)
        links = ''
        # 输出匹配结果
        for match in matches:
            # print(json.loads(match))
            links = json.loads(match).get('@graph')

        current_links = links
        if self.previous_links is not None and current_links == self.previous_links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理

        self.previous_links = current_links  # 更新上一页的链接列表

        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            # print(item)
            items = Item()
            items.article_url = item.get("url")
            items.title = item.get("headline")
            items.country = self.country
            items.pubtime = item.get("datePublished")
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.timesofmalta.com/search"
        params = {
            "keywords": f"{current_keyword}",
            "include%5B0%5D": "title",
            "include%5B1%5D": "body",
            "tags%5B0%5D": "49",
            "tags%5B1%5D": "712",
            "tags%5B2%5D": "310",
            "range": "all_times",
            "from": "2000-01-01",
            "until": "2025-01-06",
            "sort": "score",
            "order": "desc",
            "author": "0",
            "page": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        pattern = r'<script type="application/ld\+json" id="article-ld">(.*?)</script>'

        # 使用re.DOTALL让.匹配包括换行符在内的所有字符
        matches = re.findall(pattern, response.text, re.DOTALL)
        content = ''
        # 输出匹配结果
        for match in matches:
            print(match)
            content = json.loads(match)['@graph'][0].get("articleBody")
        # print(content)
        items.content = content
        # items.content = "".join(response.xpath("//p[@data-mrf-recirculation='Article body inline links']/text()").extract())
        items.author = ''
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
