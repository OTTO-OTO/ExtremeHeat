import json
import random
from datetime import datetime

import feapder
from feapder import Request, Response
from NewsItems import SpiderDataItem


class ChinaSpdier3(feapder.AirSpider):
    def start_requests(self):
        cookies = {
            "cna": "KRftHzKCtnsCAavV0T0oKATw",
            "sca": "7bc0f982",
            "atpsida": "8a1059a1d5ea907e80bbea84_1734758582_1"
        }

        keyword_ = '干旱'
        """
        hot:1-106
        rainstorm:6
        hyperthermia/high temperature:1-50
        drought:10
        """
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

            url = url = "https://search.cctv.com/search.php"
            yield feapder.Request(url, callback=self.parse_url, params=params, cookies=cookies,
                                  download_midware=self.download_midware)

    def parse_url(self, request, response):
        keyword_ = '干旱'
        url_lists = response.xpath("//h3[@class='tit']//span/@lanmu1").extract()
        print(url_lists)
        # print(json.loads(url_lists))
        for item in url_lists:
            items = SpiderDataItem()
            items.keyword = keyword_
            items.country = 'China'
            items.article_url = item
            yield feapder.Request(url=item, callback=self.parse_detail, download_midware=self.download_midware,
                                  items=items)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = "china"  # 表名
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
        print(content)
        if content:
            yield items

    def download_midware(self, request: Request):
        keyword_ = 'rainstorm'
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
    ChinaSpdier3().start()
