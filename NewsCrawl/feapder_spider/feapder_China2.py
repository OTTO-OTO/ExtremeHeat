import json
import random
from datetime import datetime

import feapder
from feapder import Request, Response
from NewsItems import SpiderDataItem


class ChinaSpdier2(feapder.AirSpider):
    def start_requests(self):
        cookies = {
            "__jsluid_h": "aa21656e4c77ef9be93704ec53f95359",
            "sso_c": "0",
            "sfr": "1"
        }

        keyword_ = '干旱'
        """
        hot:1-106
        rainstorm:6
        hyperthermia/high temperature:1-50
        drought:10
        """
        for page in range(1, 1001):
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
            yield feapder.Request(url, callback=self.parse_url, data=data, cookies=cookies,
                                  download_midware=self.download_midware)

    def parse_url(self, request, response):
        keyword_ = '干旱'
        url_lists = response.json["data"].get("records")
        # print(url_lists)
        # print(json.loads(url_lists))
        for item in url_lists:
            items = SpiderDataItem()
            items.title = item.get("title")
            items.keyword = keyword_
            items.pubtime = datetime.fromtimestamp(int(item.get("displayTime"))/1000).strftime('%Y-%m-%d %H:%M:%S')
            items.country = 'China'
            items.author = item.get('author')
            items.article_url = item.get("url")
            yield feapder.Request(url=item.get("url"), callback=self.parse_detail,download_midware=self.download_midware,items=items)
    def parse_detail(self, request, response):
        items = request.items
        items.table_name = "china"  # 表名
        content = "".join(response.xpath("//div[contains(@class, 'box_con') or contains(@class, 'rm_txt_con cf')]").extract()) # class=box_con class=rm_txt_con cf
        if content:
            items.content = content
        else:
            items.content = ''
        print(content)
        if content:
            yield items


    def download_midware(self, request: Request):
        keyword_ = 'rainstorm'
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


if __name__ == '__main__':
    ChinaSpdier2().start()
