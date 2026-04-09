# -*- coding: utf-8 -*-
import json

import feapder
from feapder import Item


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[5, 8],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB="TyphoonMangkhut",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )

    )

    country = 'China'
    keywords =  [ '热带气旋山竹','热带低压山竹','热带风暴山竹', '台风山竹','飓风山竹','气旋山竹',  '风暴山竹']
    table = 'China'

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://search.cctv.com/search.php"
            params = {
                "qtext": f"{keyword}",
                "sort": "relevance",
                "type": "web",
                "vtime": "",
                "datepid": "1",
                "channel": "",
                "page": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Referer": "https://search.cctv.com/search.php?qtext=%E7%8F%8A%E7%91%9A%E7%99%BD%E5%8C%96&type=web",
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
        request.cookies = {
            "cna": "KRftHzKCtnsCAavV0T0oKATw",
            "sca": "d87bea4f",
            "atpsida": "b6e78fae1a87135ae40ee699_1736212868_2"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        if request.page >= 10:
            print(f"关键词 {current_keyword} 的第 {request.page} 页已经抓取完毕，退出当前关键字的循环")
            return None
        links = response.xpath("//h3[@class='tit']//span/@lanmu1").extract()
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            items = Item()
            items.article_url = item
            items.country = self.country
            # items.title = item.get("title")
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items,page=request.page)

        current_page = request.page + 1
        url = "https://search.cctv.com/search.php"
        params = {
            "qtext": f"{current_keyword}",
            "sort": "relevance",
            "type": "web",
            "vtime": "",
            "datepid": "1",
            "channel": "",
            "page": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath(
            "//div[contains(@class, 'cnt_bd')]//p/text() | //div[@class='content_area']//p/text() | //div[@class='body']//p/text()").extract())  # //div[@class='body']//p/text()  //div[@class='cnt_bd']//p/text() //div[@class='cnt_bd']//p/text()
        items.author = ''
        items.pubtime = response.xpath("//div[@class='info1']/text()").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
