# -*- coding: utf-8 -*-
#173
import feapder
from feapder import Item


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

    country = 'Togo'
    table = 'Togo'
    #法语
    keywords = ["Cyclone tropical", "Dépression tropicale", "Tempête tropicale", "Typhon", "Ouragan", "Cyclone", "Tempête", "Pluies diluviennes", "Inondation", "Houle", "Dommages côtiers", "Glissement", "Catastrophe géologique", "Catastrophe marine", "Vents violents", "Catastrophe de typhon", "Glissement de terrain", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://togopresse.tg/page/1/"
            params = {
                "s": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "Accept": "text/html, */*; q=0.01",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Referer": "https://togopresse.tg/?s=extr%C3%AAme",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "X-Requested-With": "XMLHttpRequest",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        request.cookies = {
            "_ga": "GA1.2.770982609.1736728589",
            "_gid": "GA1.2.16902186.1736728589",
            "burst_uid": "d8bb747f4d603777a8a25bc1b340d811",
            "__gads": "ID=d33dac6a031685c3:T=1736728579:RT=1736728969:S=ALNI_MYJ5OnyQjpV4FjBhI0kDOpD6JiyDQ",
            "__gpi": "UID=00000fe6c4621132:T=1736728579:RT=1736728969:S=ALNI_MZ_WBV2u93GvFXM0vGEXcnI4pgJng",
            "__eoi": "ID=46641de0295259df:T=1736728579:RT=1736728969:S=AA-AfjZiYA55OC-jIqhEqtIXjKbR",
            "quads_browser_width": "1920",
            "_ga_P4NSNJDXXD": "GS1.2.1736728589.1.1.1736729179.0.0.0"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//div[@class='widget-full-list-text left relative']//a/@href").extract()
        # print(links)

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
            # print(item.get("link"))
            items = Item()
            items.table_name = self.table
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://togopresse.tg/page/{current_page}/"
        params = {
            "s": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        content = "".join(response.xpath("//div[@class='theiaPostSlider_slides']//p/text()").extract())
        items.content = content
        items.author = ''
        items.pubtime = ''
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
