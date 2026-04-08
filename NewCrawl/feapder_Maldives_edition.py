import feapder
from feapder import Item
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
        MYSQL_DB="other_country_site1",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    country = 'Maldives'
    table = 'Maldives_edition'
    keywords = ["Extreme heatwave", "Extreme temperature", "Heatwave event",
                "Increase in high temperature", "High temperature impact", "High temperature", "Strong heat",
                "Temperature rise", "Heat event", "Temperature increase", "Heavy rainfall", "Heavy precipitation",
                "Torrential rain", "Extreme rainfall", "Drought", "Severe drought", "Long-term drought",
                "Water shortage", "Power outage", "High temperature power outage", "Heatwave power outage",
                "Power outage caused by high temperature", "Fire", "High temperature fire", "Heat fire",
                "Temperature fire", "Heat-induced fire", "Agricultural impact", "Heatwave agriculture", "Crop damage",
                "Agricultural heat stress", "Hypoxia", "Heatstroke", "Heat-induced heatstroke",
                "High temperature hypoxia", "High temperature heatstroke", "Traffic impact", "High temperature traffic",
                "Heatwave traffic", "Temperature traffic", "Ecological disaster", "Heat disaster",
                "High temperature environment", "Impact of heat on biodiversity", "Heatwave ecology", "Pollution",
                "High temperature pollution", "Heat pollution", "Temperature pollution", "Coral bleaching",
                "High temperature coral reefs", "Temperature coral bleaching"]


    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://edition.mv/search"
            params = {
                "keyword": f"{keyword}",
                "page": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://edition.mv/search?keyword=Extreme%20heatwave&page=2",
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
            "_ga": "GA1.1.1177545943.1736131210",
            "cf_clearance": "NjXpAVTJtTIrw7vUpY8jAoG8eozPQx9KLPuf3tsRLKI-1736307223-1.2.1.1-bzSiig7NVpfGlQyHkHBmuHaeC8VAlCJtt9S5XxURgSWmRWK2tshnQWqy5ed3RylC3NMVb4DW0Fpswx7WKLdRW55PGoDl1p9Ut04T4EUZFFM1VwQYEcy4zwzbjicQHnNUTjPPbncbme1Tz08cnN4YiMV.ue50vsmQUnymtS4IIWOLb7KMGuaa.VRThP4sZig08jIJpgRISSLM3IaZOPm9MN4lhaowqENTnqGNOUFuX4Q8cRZbWmGLyZFOEoWp1sqwFmnYYESisYUpz938r4NP3BFaWEY5eS6_dhmShDang0G7quXuQGPAxiSh.M7.zc6ZqKgN7c_6zyD5j2.7s3eM2yw6XyATKadSzavkrMQ_jbBLPdUq6BEA6I2oMi75LzBySBk5zAHhJYNTmm.4htCnvQ",
            "_ga_Q9GBHQBB3Z": "GS1.1.1736307234.2.1.1736307263.31.0.0",
            "XSRF-TOKEN": "eyJpdiI6IkFxcWxFMS9MOTBtdnNRTWdkZEJ6SVE9PSIsInZhbHVlIjoiWFVleExjaFI5KzZaQlAyNW50K21jSGs0VDlFMENhWUJUdmFLYmMrdGYyNzB6bHlVeFcxZmZib1VXVWFZd2E4eUhuTjlpem8xSVd2WHBqd0pmSUdyK2JuOEJtd3FmdkpPaEwwRWg5aFVHQkhSNjRKQi8xV2s2WWxGUDBVYzZHZ0ciLCJtYWMiOiI4MDQwZDYyYmI2OGJiOTk1YmZmOTMyYjQ0ZGJiMDA3OWVlZGRkOWRmYjE5MTI2MjlkM2I3ZDBiYTI5NTJhYzFkIiwidGFnIjoiIn0%3D",
            "edition_session": "eyJpdiI6IjVhb0RiaDhVUjlPZnhobXZIODBsOXc9PSIsInZhbHVlIjoiWkxHK3E5VlFCcGlzMDY3SEVBaFNOcFNEU3dOeXFzOS9GMmVnMGF4ZHpCRzB1Nis5a0paN3FrdDBoQ25iVFlUY1grY1E3dnUzWWplRUdlSUNyRFdwRGFGL1J5WEYxSnFDb1IyM2RXZlZpRjVPVWc2YjA1WHkrMjVrcFNxZEIyb0IiLCJtYWMiOiJlNTQwYmU3MWQzNjlkNmViODk1ZTdmYzZjMTU1ZDkzN2EwOTc2MmE5NWMwMjQ1ZGI2NmYxYjJmNzMwMTM1Zjc3IiwidGFnIjoiIn0%3D"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")

        links = response.xpath("//div[@class='featured-card']/h2/a/@href").extract()
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            print(item)
            items = Item()
            items.article_url =item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://edition.mv/search"
        params = {
            "keyword": f"{current_keyword}",
            "page": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//p[@class='b-para']//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//div[@class='d-time']/text()").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
