import json

import feapder
from feapder import Item
from curl_cffi import requests
from lxml import etree
import re


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[6, 10],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB="TropicalCyclone",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'San Marino'
    table = 'San_Marino'
    #意大利语
    keywords = ["Ciclone tropicale", "Depressione tropicale", "Tempesta tropicale", "Tifone", "Uragano", "Ciclone", "Tempesta", "Forti piogge", "Alluvione", "Mareggiata", "Danni costieri", "Smottamento", "Disastro geologico", "Disastro marino", "Forte vento", "Disastro dovuto a tifone", "Frana di fango", "Frana"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://www.sanmarinortv.sm/risultato-ricerca"
            params = {
                "term": f"{keyword}",
                "type": "articles",
                "page": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.sanmarinortv.sm/risultato-ricerca?type=articles&term=temperature+estreme",
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
            "_ga": "GA1.1.1262928597.1736406332",
            "grantCookiesConf": "{\"smartsupp\":true,\"livebeep\":true,\"hotjar\":true,\"feedaty\":true,\"awin\":true,\"criteo\":true,\"transactionale\":true,\"zendesk\":true,\"clerk_ecommerce\":true,\"facebook_widget\":true,\"facebook_chat\":true,\"google_ads\":true,\"pixel_facebook\":true,\"pixel_linkedin\":true,\"titanka_tracking\":true,\"google_user_id\":true,\"google_analytics\":true,\"yandex\":true,\"ms_clarity\":true,\"microsoft_advertising\":true,\"tiktok\":true,\"matomo\":true}",
            "cookieCategoriesGrant": "{\"technical\":true,\"analytics\":true,\"marketing\":true}",
            "cookieUserLog": "{\"id\":\"c3ae141a-1b756f55-d660b062ac\",\"cid\":\"1.3e0li3n2h1\",\"utc\":1736406363023}",
            "CookiePolicy": "1",
            "_fbp": "fb.1.1736406363370.802440447196509884",
            "vido_first_impression": "3000000014",
            "__qca": "I0-1560645472-1736406447513",
            "cto_bundle": "ZHVsRV9PT1JQUkJIcFlMNFJhcloxd0pnNGdUM0lyY3Nqd3JrTkVwOWhrJTJGYnUxZTBDR1VWUmlxaFFndGkzUjNzViUyRk1tQ1lwSXFmaDF1VXFxV0FuU1JETXlJWkxpNnRpRGZscFBKMWpqUk1jbDh4MXRTaHNCOXNZJTJGTWUzOUhRclhPRVB5czl5VGRTMWx1Q1pHb1duV2M3SVdDY0ElM0QlM0Q",
            "cto_bidid": "jsSWbl9ZJTJCZHcwOUJ4VHBsMCUyQlk5d3d6N2t3RndOd1RKVVFLYlNNRnhVMyUyQnZJQlRMdEpoR2tSa1pSTTJOT1dNNEU3NmpsMFJ5ajZkT3JzZW9GWmNnajJReHh1bzBCMGRJdVRNQzg2M1diNW9tQW1UVmpOeXBPJTJCdVNNNzclMkYlMkJ4anhwU3hYdg",
            "cto_dna_bundle": "bTg6JV9PT1JQUkJIcFlMNFJhcloxd0pnNGdSZjV4Y3BGUGg5dk5mZU01djluS0glMkJKUzQ4QmYzM2xMTm9rVk9LbEVQaHN0eHB6WnQ4SGJCcWdzOEltckxmZCUyQmclM0QlM0Q",
            "FCNEC": "%5B%5B%22AKsRol93uRdsUPY40pxU-2ic9mGriKCgznvSg1R0EMhJoyMSpUohCqnQ9XvtXa_j1YVtlGvLVE9KhHIP6AR1gnWM1jUp-zy8jL0hWO_EYsXrrAa0Ybmi-m_RORmdfnmBdamzu3Co1vyUJBtR5t0UBYip4ybTTMCQyw%3D%3D%22%5D%5D",
            "_ga_QCTL9K7JJN": "GS1.1.1736406331.1.1.1736406502.45.0.0"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//article//div[@class='action-container']/a/@href").extract()
        # print(data)

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
        if request.page >= 10:
            print(f"关键词 {current_keyword} 的第 {request.page} 页已经抓取完毕，退出当前关键字的循环")
            return None
        for item in links:
            # print(item.get("url"))
            items = Item()
            items.table_name = self.table
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.sanmarinortv.sm/risultato-ricerca"
        params = {
            "term": f"{current_keyword}",
            "type": "articles",
            "page": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        content = "".join(response.xpath("//div[@class='description js-font-size-action']//text()").extract())
        items.content = content
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
