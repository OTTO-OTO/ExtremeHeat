import json

import feapder
from NewsItems import SpiderDataItem
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
    country = 'El Salvador'
    keywords =  ["Ciclón tropical", "Depresión tropical", "Tormenta tropical", "Tifón", "Huracán", "Ciclón", "Tormenta", "Lluvias intensas", "Inundación", "Marea", "Daños costeros", "Deslizamiento", "Desastre geológico", "Desastre marino", "Vientos fuertes", "Desastre de tifones", "Derrumbes de lodo", "Deslizamiento de tierra"]
    #西班牙语
    table = 'El_Salvador'

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = f"https://www.diariocolatino.com/page/{page}/"
            params = {
                "s": f"{keyword}",
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.diariocolatino.com/?s=Tropical%20",
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
            "_ga": "GA1.1.1600137552.1735110479",
            "_gcl_au": "1.1.437772522.1735110480",
            "__gads": "ID=c2ba271bf04bcdb6:T=1735110478:RT=1735110478:S=ALNI_MYY59Ww50GEApVSEHhwcbOFl1oMDA",
            "__gpi": "UID=00000fb7646346ef:T=1735110478:RT=1735110478:S=ALNI_MbjAWXh0hvIW7JnYBmPix0X2svLUg",
            "__eoi": "ID=ad5b31a16b12dc98:T=1735110478:RT=1735110478:S=AA-AfjYp_9DdpgnvIWH5VRhmjNPN",
            "_ga_CNY9PK62XE": "GS1.1.1735110479.1.1.1735110535.0.0.0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//article/h2/a/@href").extract()
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理

        if request.page >= 10:
            print(f"关键词 {current_keyword} 的第 {request.page} 页已经抓取完毕，退出当前关键字的循环")
            return None

        for url in links:
            print(url)
            items = SpiderDataItem()
            items.article_url = url
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://www.diariocolatino.com/page/{current_page}/"
        params = {
            "s": f"{current_keyword}",
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1//text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='entry']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
