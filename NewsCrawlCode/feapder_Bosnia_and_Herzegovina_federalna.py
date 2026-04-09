import feapder
from feapder import Item
from lxml import etree


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[8, 10],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB="other_country_site1",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    country = 'Bosnia and Herzegovina'
    table = 'Bosnia_and_Herzegovina_federalna'
    keywords = [
        'Ekstremno', 'Vruće', 'Visoka temperatura', 'Nagli kiš', 'Suša',
        'Prekid struje zbog vrućine', 'Pozar', 'Zračna zagađenja', 'Klimatske promjene',
        'Smanjenje poljoprivrednog prinosa', 'Oksigenska nedostatak', 'Visoka temperatura utiče na promet',
        'Ekološki katastrofa', 'Klimatske promjene utječu na gospodarstvo', 'Mornarska vruća vala',
        'Zagađenje visokom temperaturom', 'Koralji'
    ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.federalna.ba/search"
            params = {
                "q": f"{keyword}",
                "page": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.federalna.ba/search?q=Ekstremno",
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
            "_ga": "GA1.1.1319992459.1734510453",
            "_ga_5X2ES5FHMB": "GS1.1.1735782971.3.1.1735783133.0.0.0",
            "XSRF-TOKEN": "eyJpdiI6IkZncGtpbUN1SnQrSFBVMlIxeVwvT2tRPT0iLCJ2YWx1ZSI6IkM5RmJVOFNSUzJydUJzYUg4SGtZWjM3cnhObUdNUWJlREYwZzRra1BDRXk5elM4RnFBRzlRTTBQckVsXC9PVXNaIiwibWFjIjoiNTM2M2Y2MGNlNjJiZjAxY2Q3NjkyNWM4ZDViOGNkYmQ4NjAxZmYyZjM4YjU3ZDc2YjQ5ZjgwYzcyNmEyMjEzYiJ9",
            "federalna_session": "eyJpdiI6IjZsMmIyUWFuZ2JMZmFHZ2JDblZEaVE9PSIsInZhbHVlIjoiZTVxclh4ZFZVRjVGRUJFZTFxbVcwM2lMYmR0bFp0N1FVWGtaVW52OVMzcGRucU9BMXQ0b053RmswMjl1VjZpdCIsIm1hYyI6IjUzM2ViMWQ2NDdlNjVhOGVhM2M0YjU1OTMxNzk5YmUwMjc1YmRlNTZhNDBkMzM2MDE5M2I5NjI0ZmIxMDI4ZWQifQ%3D%3D"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//div[@class='media']//h4/a/@href").extract()
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            print(item)
            items = Item()
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.federalna.ba/search"
        params = {
            "q": f"{current_keyword}",
            "page": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='lead']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//div[@class='article-meta mb-3']/span/text()").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
