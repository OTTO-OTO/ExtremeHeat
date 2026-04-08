import json
import re

import feapder
from NewsItems import SpiderDataItem
from lxml import etree


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[5, 8],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
    )

    country = 'Jamaica'
    table = 'Jamaica_jamaicaobserver'
    keywords = ['Extreme','Heat','High Temperature','Heavy Rain','Drought','Power Outage from Heat','Fire','Air Pollution','Climate Change','Crop Yield Reduction','Oxygen Deficiency','High Temperature Affecting Traffic','Ecological Disaster','Climate Change Affecting Economy','Marine Heatwave','High Temperature Pollution','Coral']

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.jamaicaobserver.com/page/1/"
            params = {
                "showResults": "1",
                "Action": "Search",
                "Archive": "False",
                "Order": "Desc",
                "y": "",
                "from_date": "",
                "to_date": "",
                "type_get_category": "all",
                "orderby_from_url": "",
                "s": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.jamaicaobserver.com/?showResults=1&Action=Search&Archive=False&Order=Desc&y=&from_date=&to_date=&type_get_category=all&orderby_from_url=&s=Flooding",
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
            "_ga": "GA1.1.838061680.1735960351",
            "_pk_id.187.f384": "0e1ce6ba6e18626f.1735960353.",
            "_pk_ses.187.f384": "1",
            "__AP_SESSION__": "feb58f88-b898-4b36-818f-bfb5db412dd7",
            "_pubcid": "37dee447-611f-496f-b355-cf5bc16ded2c",
            "_pubcid_cst": "zix7LPQsHA%3D%3D",
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "panoramaId_expiry": "1736565147643",
            "panoramaId": "0010a56387b454fb99733e574a5e16d539386232f10af484e6cddb3c256de10d",
            "panoramaIdType": "panoIndiv",
            "__qca": "P0-1028282957-1735960355822",
            "pbjs-unifiedid": "%7B%22TDID%22%3A%22ea499b40-6dd6-47bc-804c-49e968bdabef%22%2C%22TDID_LOOKUP%22%3A%22FALSE%22%2C%22TDID_CREATED_AT%22%3A%222025-01-04T03%3A12%3A31%22%7D",
            "pbjs-unifiedid_cst": "zix7LPQsHA%3D%3D",
            "_ga_SFTZ8R89PJ": "GS1.1.1735960351.1.1.1735960365.46.0.0",
            "_awl": "2.1735960357.5-e448c950efb405dcf1322af02352e538-6763652d617369612d6561737431-0",
            "cto_bundle": "3d93Ml9PT1JQUkJIcFlMNFJhcloxd0pnNGdWY0Q3NFhOSWtKV3JLRFVuYUwzaTB0U0ltcHBsdmdUNkt5OENNeHhBQzVjVXdJJTJCUXZSbEJxcVpDdk1xZzRFUENHZzh1VWdySm5TOTh1SVpXZk1EZjZHdTJDN1V4ZHoxR3ZONHpyMVZrdWxmQ3RIUzJYa01jVTdxSXVDVE56JTJCbXVMQXJlYjJ0NXVBQ3VXTTQxRUFreENFJTNE",
            "cto_bidid": "tq4Q4V80Z3BVV3FiemJUT0lraHIwekdjcndmR3pEaWJJMmh1MGYxSjhodjJCT1VNdXdJaFBWcVozdERST3kwRHpsRmp3eWduNlIyeFRMY0lPbE1EZ3FoV3NvZkRUYldqQ0laWkYlMkZYZlJHQ3MxRFhTSG10JTJCV2dxc2ZDWSUyQlJ1UThCRFphVg",
            "cto_dna_bundle": "XOZcsl9PT1JQUkJIcFlMNFJhcloxd0pnNGdSZjV4Y3BGUGg5dk5mZU01djluS0glMkJKUzQ4QmYzM2xMTm9rVk9LbEVQaHN2WTk0OEIyeGt3QTh3JTJCeVVUU3hjRlElM0QlM0Q"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//div[@class='pre-content']//div[@class='title']/a/@href").extract()
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            # print(item)
            # print(item.get("title"))
            items = SpiderDataItem()
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://www.jamaicaobserver.com/page/{current_page}/"
        params = {
            "showResults": "1",
            "Action": "Search",
            "Archive": "False",
            "Order": "Desc",
            "y": "",
            "from_date": "",
            "to_date": "",
            "type_get_category": "all",
            "orderby_from_url": "",
            "s": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        # print(response.text)
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//article//div/p/text()").extract())
        items.author = ''
        items.pubtime = ''
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
