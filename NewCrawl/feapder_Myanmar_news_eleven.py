import json

import feapder
from NewsItems import SpiderDataItem
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
        MYSQL_DB="TropicalCyclone",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Myanmar'
    table = 'Myanmar'
    #英语
    keywords =["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://news-eleven.com/search/node/all"
            params = {
                "search": f"{keyword}",
                "field_author_name_value": "",
                "field_custom_post_date_value%5Bmin%5D%5Bdate%5D": "",
                "field_custom_post_date_value%5Bmax%5D%5Bdate%5D": "",
                "page": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://news-eleven.com/search/node/all?search=Climate+Change&field_author_name_value=&field_custom_post_date_value%5Bmin%5D%5Bdate%5D=&field_custom_post_date_value%5Bmax%5D%5Bdate%5D=",
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
            "_ga": "GA1.1.806668582.1736148675",
            "cf_clearance": "ZL.yyapsAEg9WoXhbk6eiha347ilNQuokBc50dw.Eck-1736149407-1.2.1.1-Fc5NWcms9_jOmk3AljgHj08W4PxPdb.IhMe8fQoI9xHfuiJMF2d8OoVFGxB6g9U8YP5w5LzdJtimJdQ_oRAkIeeIWZJVhZSobTKzq7wzdXJeGfWcOmzHYT401o7L35MyMiyJtt.xg91NEg37HO4biOgWUhVvSvXXQ3PIhuOU6FIEvsuIS45QEE03SM5A.q.yyuwNn1jR4BZqyPYubPqKM6Bhl8l93_etnRPKrEBV1tYlrpXx7PmSmxM2hdZLlQ1zQeF.Br2nGTrCPTkjRU4EJgYi7XzregIqJR.PHaiUdTe5yPl16l28bcMowM9K8SRxrC9cXXHuywcXPcLHjTsksGx.mpZXD8vAMp7rC1yU_0vScKNBNBJ4AvRXfyp6M0KWvtNp1_4SCPyl.9nAFG0cjQ",
            "_ga_S4PVC65K1X": "GS1.1.1736148675.1.1.1736149761.53.0.0"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//div[@class='search-title']/a/@href").extract()
        current_page = request.page

        current_links = links
        if self.previous_links is not None and current_links == self.previous_links:
            print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理

        self.previous_links = current_links  # 更新上一页的链接列表
        if request.page >= 10:
            print(f"关键词 {current_keyword} 的第 {request.page} 页已经抓取完毕，退出当前关键字的循环")
            return None
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            print(item)
            items = SpiderDataItem()
            items.table_name = self.table
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://news-eleven.com/search/node/all"
        params = {
            "search": f"{current_keyword}",
            "field_author_name_value": "",
            "field_custom_post_date_value%5Bmin%5D%5Bdate%5D": "",
            "field_custom_post_date_value%5Bmax%5D%5Bdate%5D": "",
            "page": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='node-content']//p//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//span[@class='date-display-single']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
