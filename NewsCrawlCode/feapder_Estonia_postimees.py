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

    country = 'Estonia'
    table = 'Estonia_postimees'
    keywords =  ["heatwave",
                 "Extreme heatwave", "High temperature", "Extreme temperature", "Heatwave event", "Increase in high temperature", "High temperature impact", "Strong heat", "Temperature rise", "Heat event", "Temperature increase", "Heavy rainfall", "Heavy precipitation", "Torrential rain", "Extreme rainfall", "Drought", "Severe drought", "Long-term drought", "Water shortage", "Power outage", "High temperature power outage", "Heatwave power outage", "Power outage caused by high temperature", "Fire", "High temperature fire", "Heat fire", "Temperature fire", "Heat-induced fire", "Agricultural impact", "Heatwave agriculture", "Crop damage", "Agricultural heat stress", "Hypoxia", "Heatstroke", "Heat-induced heatstroke", "High temperature hypoxia", "High temperature heatstroke", "Traffic impact", "High temperature traffic", "Heatwave traffic", "Temperature traffic", "Ecological disaster", "Heat disaster", "High temperature environment", "Impact of heat on biodiversity", "Heatwave ecology", "Pollution", "High temperature pollution", "Heat pollution", "Temperature pollution", "Coral bleaching", "High temperature coral reefs", "Temperature coral bleaching"
                 ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://news.postimees.ee/search"
            params = {
                "start": "1970-01-01T06:00:00+08:00",
                "query": f"{keyword}",
                "sections": "1474,455,81",
                "fields": "body,authors,headline,keywords",
                "page": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://news.postimees.ee/search?sections=1474%2C455%2C81&query=heavy%20rain&start=1970-01-01T06%3A00%3A00%2B08%3A00&fields=body%2Cauthors%2Cheadline%2Ckeywords",
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
            "_pctx": "%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAEzICYAWAdgA4uXAMxcAnNy4BGAGySpMkAF8gA",
            "_pcid": "%7B%22browserId%22%3A%22m65wtgiiypkilkkj%22%7D",
            "cX_P": "m65wtgiiypkilkkj",
            "__pat": "7200000",
            "__pid": ".postimees.ee",
            "_cb": "DnMm3VB_KOhTH0S1U",
            "_hjSession_982110": "eyJpZCI6IjI3ODI0YjQ1LWJiOTMtNDk1My1iODIyLWE5YzJmMzY3MDVjZiIsImMiOjE3Mzc0Mjk3NjM0NTksInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MH0=",
            "cX_G": "cx%3A2yr1lld948xqm1u9hqtv1gwl1h%3A1x138eegzso2x",
            "didomi_token": "eyJ1c2VyX2lkIjoiMTk0ODZlMjMtNzcwNi02MTViLWFhYzktMTkxOTRmN2QxY2FkIiwiY3JlYXRlZCI6IjIwMjUtMDEtMjFUMDM6MjI6MzkuODU2WiIsInVwZGF0ZWQiOiIyMDI1LTAxLTIxVDAzOjIyOjQ3LjA1NVoiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwiYW1hem9uIiwic2FsZXNmb3JjZSIsImM6dHJpYmVzcmVzLWM0WENUaUZoIiwiYzpjaGFydGJlYXQiLCJjOmludGVsbGlhZCIsImM6dmlkZW9sb2d5IiwiYzpzcG90YWQtenh5TFZRQ0EiLCJjOmFuYWxpZ2h0cy03bXlFSDNlayIsImM6Z29vZ2xlYW5hLTRUWG5KaWdSIiwiYzpoYWVuc2VsYW0tRVhHblBxUUoiLCJjOmRpZ2lzZWdhcC1tZjQ0Rkp4ZCIsImM6c2NlbmVzdGVhLTZVQ2U2cHlQIiwiYzpwaWFub2h5YnItUjNWS0MycjQiLCJjOndpZGVzcGFjZS1Qa2VuMktUTiIsImM6dW5ydWx5Z3JvLW5LeUxxZEtpIiwiYzpvcHRvbWF0b24taHd6d2kzYVQiLCJjOnRyZXNlbnNhLU5VM3pGbldjIiwiYzphdm9jZXRzeXMtcTdWZGtwcmIiLCJjOnNjb290YS1FVkN3cnlDZCIsImM6bGlmZXN0cmVlLUVVRFdmTUJhIiwiYzprb2NoYXZhaW4tTkFUQzhaMmEiLCJjOnNvamVybmluYy0yeHBjR1A2TSIsImM6bWVkaWFtYXRoLXdKUXdUQVoyIiwiYzpmcmFjdGlvbmFsLW4yallERlBOIiwiYzppZ25pdGlvbm8tTFZBTVpkbmoiXX0sInB1cnBvc2VzIjp7ImVuYWJsZWQiOlsiZGV2aWNlX2NoYXJhY3RlcmlzdGljcyIsImdlb2xvY2F0aW9uX2RhdGEiXX0sInZlbmRvcnNfbGkiOnsiZW5hYmxlZCI6WyJjOnBpYW5vaW8tOGRUaFZtWG4iLCJjOnBpYW5vY29tcC1ickI0OWFZaCJdfSwidmVyc2lvbiI6MiwiYWMiOiJqOS1PT0FDc0FGd0FOd0FlZ0JHQUNtQUZrQUxvQWJBQTZnQjZBRDRBSWVBUjRCSUFDVEFFcUFKOEFZQUF4QUJwZ0RrQUhNQU84QWdZQkNnQ1BBRWpBSnVBVDRBb0VCUWdGREFLT0FXOEF1WUJnUURGQUdOQU11QVo4QTFBQnJnRGFBRzlBUE1BZXVBOXdEM3dJS0Fnc0JEZ0NKb0VYZ1JnQWpnQkh3Q1FBRWtBSkxBU2VBdUdCa1FHUndNMEF6VUJvNERZQUhCZ09JQWMwQTZNQjFRRDB3SHFnUFpBZldCQU1DQTRFRUFJTEFRYkFnNkJDc0NJZ0VSd0lrZ1JMQWltQkc4Q080RWZRSkFBU0tBa2NCS0lDVWdFdFFKZndUS0JNc0Nad0U1QUo2QVQyQW40QlFRQ2hRRkZRS05nVWdBcUNCVXdDcWdGWHdLOXdXSEJZZ0N4b0Zqd0xRQVdwQXRlQmRjQzlBRjdBTDZnWUVBd1lCaE1EQ3dHSWdNVWdZcmd4ZURHQUdOd01oZ1pRQXpNQm5NRE9vR2V3TkJ3YU5CbzREU1FHb1FOUmdhbkExZ0JyUURYSUd2UU5pQWJSQTJzQnVVRGV3Ry1nT0VBY1NBNUNCeU1EbEFITGdPWVFjeUJ6TURtb0hRd09sQWRPQTZzQjFnRHNJSGF3Ty1nZUpBOHNCNWlEMVFQV1FldkI3RUQzUUh3UVBoZ2ZUQS13Qi1zRC1ZSDl3UF9nZ0VCQVFDQkFFQ2dJR0FRTkFnb0JCVUNEY0VIWUlQUVFoZ2hNQkNrQ0ZVRUs0SVdRUXVBaGpCRE1DR3dFT3dJZ1FSRkFpZEJGS0NLaUVWWVJXQWl1QkZpQ0xvRVpvSTV3UjFBajBCSUNDUVVFaEFKREFTSkFrVkJJd0NSdUVqNFNRZ2tuQkpsQ1RZSk53U2hRbElDVWtFcVlKWUFTeGdsbEJMT0NXc0V0d0pjQVM1QWw1Qk1RQ1k4RXpvSm9RVFNBbW9CTmFDYkVFMmdKdGdUY0FtOUJPRUNjVUU1d0oxWVR2Qk8tQ2VRRTg0SjdnVDhRb0RDZ1NGQTRVRWdvTUJRZUNoR0ZDNFVNQW9raFJZRkZzS01Bb3lCUm9DallGSHNLUkFwRkJTU0NreUZLUVVwd3BWQ2xhRkxRVXVRcGdDbUtGTWdVeXdwdENtNEZPb0tlUVUtQXBfQlVFQ29jRlJVS21RcWFCVlNDck9GWjRWb0FyU0JXb0N1QUZjY0s1d3JvQlhjQ3ZFRmVzS19BcjloWUFGZ01MQlFzR0JZWEN3NExEd1dKUXNXQ3hjRmpnTEhvV1FoWkVDeVFGa3dMTElXWEJaZUN6RUZtWUxOWVdjaFo0Q3o2Rm9BV2lRdEdDMGtGcGNMVFF0UGhhaUZxVUxWQXRWaGE0RnJzTFpBdG9oYlVGdFlMYjRXNGhibkMzd0xmUVhCd3VGQzRZRnhnTGpnWEl3dVRDNVlGekFMblFYUHd1akM2VUYwOExxUXVyaGRrRjJZTHRZWGJoZHhDN29MdXdYZXd2QUM4SUY0a0x5QXZMQmVpQzlRRjdRTDNnWHh3dnNDLTBGOThMOGd2ekJmdUNfb0ZfZ0xfb1lBaGdLREJHR0NvWUxnd1poZzJHRGdNSWdZU2d3b0JoYURDLUdHd1ljQXc2aGgtR0lBTVNZWW5CaWZERklNVXdZdGd4ZmhqSUdNc01iZ3h4aGptR093TWVnWS1ReUpESXVHUndaSGd5UkJrbkRKWU1sNFpNaGs1REo0TW40WlJCbE1ES2lHVmdaV3d5eURMTUdXb011UVplQXpHQm1RRE5FR2JBTTJvWnVCbTZET0NHY1laeVF6cURPc0dkc004Z3owQm5zRFB3R2dFTkFnMERCdVdEcE1IZTRRSXdqWmhJWENUMkV0OE5pWWNRdzlMaDc3RVhHSmdNVC00cGh4VW5pc1hGb1dNaDhhb0Exb3h2T2oxeklOLVFpTXMtNWxTekxYbXhUTmx1Ylg4NEhad256bmZuOFRRYS1oYmREbzZQM3cuQUFBQSJ9",
            "euconsent-v2": "CQLlOMAQLlOMAAHABBENBZFsAP_gAAAAAAggKwtX_G__bXlr8X736ftkeY1f99h77sQxBhbJk-4FzLvW_JwX32EzNA36tqYKmRIAu3TBIQNlHJDURVCgaogVrzDMaEyUoTtKJ6BkiFMRY2dYCFxvm4tjeQCY5vr991d52R-t7dr83dzyy4hnv3a9_-S1WJCdA5-tDfv9bROb-9IO9_x8v4v4_N7pE2_eT1l_tWvp7D9-ctv_9XX99_fbff9Pn_-uB_-_X__f_H36AAAAYpABgACCsJKADAAEFYQkAGAAIKwloAMAAQVhIQAYAAgrCOgAwABBWEAA.f_wAAAAAAAAA",
            "_ga": "GA1.1.835294703.1737429762",
            "pmCAcp": "1",
            "_hjSessionUser_982110": "eyJpZCI6Ijg2MTZkNzM5LThkZWQtNWRlZi1hYWFmLTYxZTI3OTcxYTIzNSIsImNyZWF0ZWQiOjE3Mzc0Mjk3NjM0NTksImV4aXN0aW5nIjp0cnVlfQ==",
            "_fbp": "fb.1.1737429776412.86281324111316709",
            "LANG": "en_US",
            "__pil": "en_US",
            "_cb_svref": "https%3A%2F%2Fwww.postimees.ee%2F",
            "__pvi": "eyJpZCI6InYtbTY1d3RnaXNkYWdrZGR2ZSIsImRvbWFpbiI6Ii5wb3N0aW1lZXMuZWUiLCJ0aW1lIjoxNzM3NDI5ODM4MzU2fQ%3D%3D",
            "__adblocker": "false",
            "__tbc": "%7Bkpex%7DzXAufgJWNmxy9SUKWhzuYlRKLGaohT6DwlqRJAer12SqD6mg9yVPiIc-nZh5KORi",
            "xbc": "%7Bkpex%7DFi8s0s3zJx_8vsj8QLRkfnbzi2xBsgmEUH5VjEnG1_9BncRHfRry-sQ7oGbqUQiguDkbH0ZXhIANf9oyU2sDPg",
            "_chartbeat2": ".1737429761604.1737429839331.1.DpxryTBSLiacD_mq0QDuNb_6DJ496f.3",
            "_ga_6K97KKRNRY": "GS1.1.1737429762.1.1.1737429839.59.0.0",
            "_chartbeat5": "966|3479|%2Fsearch|https%3A%2F%2Fnews.postimees.ee%2Fsearch%3Fstart%3D1970-01-01T06%253A00%253A00%252B08%253A00%26query%3Dheavy%2520rain%26sections%3D1474%252C455%252C81%26fields%3Dbody%252Cauthors%252Cheadline%252Ckeywords%26page%3D2|BE6xQGCGRjJmQak2p396Oyt_DjZ||c|DUWzsiOhDbxBwGIAdBgeWuRDsox38|postimees.ee|"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//article/a/@href").extract()
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
            print(item)
            items.table_name = self.table # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://news.postimees.ee/search"
        params = {
            "start": "1970-01-01T06:00:00+08:00",
            "query": f"{current_keyword}",
            "sections": "1474,455,81",
            "fields": "body,authors,headline,keywords",
            "page": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='article-body']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
