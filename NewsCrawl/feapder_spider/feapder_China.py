import json
import random
from datetime import datetime

import feapder
from feapder import Request
from NewsItems import SpiderDataItem
from urllib.parse import urljoin
import re


class ChinaSpdier(feapder.AirSpider):
    def __init__(self):
        super().__init__()
        # 避免使用logger，直接使用print
    def start_requests(self):
        keywords = [
            '极端', '热', '高温', '暴雨', '干旱', '高温停电',
            '火灾', '空气污染', '气候变化', '农作物减产', '缺氧',
            '中暑', '高温影响交通', '生态灾难', '气候变化影响经济',
            '海洋热浪', '高温污染', '珊瑚'
        ]
        
        for keyword in keywords:
            # 数据源1: news.cn
            yield from self.start_requests_news_cn(keyword)
            
            # 数据源2: people.cn
            yield from self.start_requests_people_cn(keyword)
            
            # 数据源3: cctv.com
            yield from self.start_requests_cctv_com(keyword)
    
    def start_requests_news_cn(self, keyword_):
        cookies = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Referer": "https://so.news.cn/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }

        for page in range(1, 101):  # 减少页数，避免请求过多
            params = {
                "lang": "cn",
                "curPage": f"{page}",
                "searchFields": "0",
                "sortField": "0",
                "keyword": f"{keyword_}"
            }

            url = "https://so.news.cn/getNews"
            yield feapder.Request(url, callback=self.parse_url_news_cn, params=params, cookies=cookies,
                                  download_midware=self.download_midware_news_cn, meta={"keyword": keyword_})
    
    def start_requests_people_cn(self, keyword_):
        cookies = {
            "__jsluid_h": "aa21656e4c77ef9be93704ec53f95359",
            "sso_c": "0",
            "sfr": "1"
        }

        for page in range(1, 101):  # 减少页数，避免请求过多
            data = {
                "key": f"{keyword_}",
                "page": page,
                "limit": 10,
                "hasTitle": True,
                "hasContent": True,
                "isFuzzy": False,
                "type": 0,
                "sortType": 0,
                "startTime": 0,
                "endTime": 0
            }

            url = "http://search.people.cn/search-platform/front/search"
            data = json.dumps(data, separators=(',', ':'))
            yield feapder.Request(url, callback=self.parse_url_people_cn, data=data, cookies=cookies,
                                  download_midware=self.download_midware_people_cn, meta={"keyword": keyword_})
    
    def start_requests_cctv_com(self, keyword_):
        cookies = {
            "cna": "KRftHzKCtnsCAavV0T0oKATw",
            "sca": "7bc0f982",
            "atpsida": "8a1059a1d5ea907e80bbea84_1734758582_1"
        }

        for page in range(1, 31):
            params = {
                "qtext": f"{keyword_}",
                "sort": "relevance",
                "type": "web",
                "vtime": "",
                "datepid": "1",
                "channel": "",
                "page": f"{page}"
            }

            url = "https://search.cctv.com/search.php"
            yield feapder.Request(url, callback=self.parse_url_cctv_com, params=params, cookies=cookies,
                                  download_midware=self.download_midware_cctv_com, meta={"keyword": keyword_})

    def parse_url_news_cn(self, request, response):
        keyword_ = request.meta.get("keyword")
        try:
            # 检查响应状态码
            if response.status_code != 200:
                print(f"news.cn API returned status code: {response.status_code}")
                return
            
            # 尝试解析JSON
            data = response.json
            url_lists = data.get("content", {}).get("results", [])
            
            for item in url_lists:
                items = SpiderDataItem()
                items.table_name = "china"
                items.keyword = keyword_
                items.pubtime = item.get('pubtime')
                items.article_url = item.get("url")
                if items.article_url:
                    yield feapder.Request(url=items.article_url, callback=self.parse_detail_news_cn,
                                          download_midware=self.download_midware_news_cn, items=items, meta={"keyword": keyword_})
        except Exception as e:
            print(f"Error parsing news.cn response: {e}")
            print(f"Response text: {response.text[:500]}")

    def parse_detail_news_cn(self, request, response):
        keyword_ = request.meta.get("keyword")
        items = request.items
        items.table_name = "china"  # 表名
        items.title = response.xpath("//title/text()").extract_first()
        items.keyword = keyword_
        author = response.xpath("//span[@class='editor']/text()").extract_first()
        if author:
            items.author = author
        else:
            items.author = ''
        content = "".join(response.xpath("//div[@id='detail']//p//text()").extract())
        if content:
            items.content = content
        else:
            items.content = ''
        items.country = "China"
        if content:
            yield items

    def parse_url_people_cn(self, request, response):
        keyword_ = request.meta.get("keyword")
        try:
            # 检查响应状态码
            if response.status_code != 200:
                print(f"people.cn API returned status code: {response.status_code}")
                return
            
            # 尝试解析JSON
            data = response.json
            url_lists = data.get("data", {}).get("records", [])
            
            for item in url_lists:
                items = SpiderDataItem()
                items.table_name = "china"
                items.title = item.get("title")
                items.keyword = keyword_
                try:
                    items.pubtime = datetime.fromtimestamp(int(item.get("displayTime"))/1000).strftime('%Y-%m-%d %H:%M:%S')
                except:
                    items.pubtime = ''
                items.country = 'China'
                items.author = item.get('author', '')
                items.article_url = item.get("url")
                if items.article_url:
                    yield feapder.Request(url=items.article_url, callback=self.parse_detail_people_cn, 
                                          download_midware=self.download_midware_people_cn, items=items, meta={"keyword": keyword_})
        except Exception as e:
            print(f"Error parsing people.cn response: {e}")
            print(f"Response text: {response.text[:500]}")

    def parse_detail_people_cn(self, request, response):
        items = request.items
        items.table_name = "china"  # 表名
        content = "".join(response.xpath("//div[contains(@class, 'box_con') or contains(@class, 'rm_txt_con cf')]").extract())
        if content:
            items.content = content
        else:
            items.content = ''
        if content:
            yield items

    def parse_url_cctv_com(self, request, response):
        keyword_ = request.meta.get("keyword")
        try:
            url_lists = response.xpath("//h3[@class='tit']//a/@href").extract()
            
            for item in url_lists:
                items = SpiderDataItem()
                items.table_name = "china"
                items.keyword = keyword_
                items.country = 'China'
                items.article_url = item
                if items.article_url:
                    yield feapder.Request(url=items.article_url, callback=self.parse_detail_cctv_com, 
                                          download_midware=self.download_midware_cctv_com, items=items, meta={"keyword": keyword_})
        except Exception as e:
            print(f"Error parsing cctv.com response: {e}")

    def parse_detail_cctv_com(self, request, response):
        items = request.items
        items.table_name = "china"  # 表名
        items.title = response.xpath("//title/text()").extract_first()
        content = "".join(response.xpath("//div[@class='content_area']//p/text()").extract())
        if content:
            items.content = content
        else:
            items.content = ''
        pubtime_str = response.xpath("//div[@class='info']//a/text()").extract_first()
        if pubtime_str:
            items.pubtime = pubtime_str.split("|")[-1]
        else:
            items.pubtime = ''
        if content:
            yield items

    def download_midware_news_cn(self, request: Request):
        request.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Referer": "https://so.news.cn/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        return request

    def download_midware_people_cn(self, request: Request):
        request.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Content-Type": "application/json;charset=UTF-8",
            "Origin": "http://search.people.cn",
            "Referer": "http://search.people.cn/s?keyword=%E7%83%AD&st=0&_=1734742959010",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        return request

    def download_midware_cctv_com(self, request: Request):
        request.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Referer": "https://search.cctv.com/search.php?qtext=%E7%83%AD&type=web",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        return request


if __name__ == '__main__':
    ChinaSpdier().start()
