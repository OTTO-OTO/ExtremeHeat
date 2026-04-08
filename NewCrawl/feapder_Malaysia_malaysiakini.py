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
    country = 'Malaysia'
    table = 'Malaysia_malaysiakini'
    keywords = [ "Extreme heatwave","Extreme temperature", "Heatwave event", "Increase in high temperature", "High temperature impact", "High temperature", "Strong heat", "Temperature rise", "Heat event", "Temperature increase", "Heavy rainfall", "Heavy precipitation", "Torrential rain", "Extreme rainfall", "Drought", "Severe drought", "Long-term drought", "Water shortage", "Power outage", "High temperature power outage", "Heatwave power outage", "Power outage caused by high temperature", "Fire", "High temperature fire", "Heat fire", "Temperature fire", "Heat-induced fire", "Agricultural impact", "Heatwave agriculture", "Crop damage", "Agricultural heat stress", "Hypoxia", "Heatstroke", "Heat-induced heatstroke", "High temperature hypoxia", "High temperature heatstroke", "Traffic impact", "High temperature traffic", "Heatwave traffic", "Temperature traffic", "Ecological disaster", "Heat disaster", "High temperature environment", "Impact of heat on biodiversity", "Heatwave ecology", "Pollution", "High temperature pollution", "Heat pollution", "Temperature pollution", "Coral bleaching", "High temperature coral reefs", "Temperature coral bleaching" ]


    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.malaysiakini.com/api/en/search/1"
            params = {
                "searchQuery": "heavy rain",
                "queryCategory": "",
                "queryDateFrom": "2000-01-01",
                "queryDateTo": "2025-01-01",
                "sort": "desc"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=1, i",
            "referer": "https://www.malaysiakini.com/en/search?keywords=heavy+rain&category=&startDate=2000-01-01&endDate=2025-01-01&sort=desc&page=0",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "fontSize": "text-xl",
            "_gid": "GA1.2.1884873249.1736130225",
            "_fbp": "fb.1.1736130225676.558941044612349923",
            "__nl_analytic": "KiI8UIi1_OpVvVnF0a7AUWlq",
            "__gads": "ID=04cfe8b13e624d13:T=1736130217:RT=1736130217:S=ALNI_MY_t_uqqbYL1H-9y2DT-ZO9cssS_A",
            "__gpi": "UID=00000fd6292bece9:T=1736130217:RT=1736130217:S=ALNI_Mbl--Nkk6Fiex7mSLiRDebz-MX8vA",
            "__eoi": "ID=015f6fb0412270ae:T=1736130217:RT=1736130217:S=AA-AfjYoTl1leeQAc13vwa9N2Tv2",
            "iUUID": "9cd2e7b1e9d34237898b1f76050f47e1",
            "innity.dmp.6.sess.id": "132505030.6.1736130231189",
            "innity.dmp.cks.innity": "1",
            "cto_bundle": "4zcVCl9PT1JQUkJIcFlMNFJhcloxd0pnNGdjU3Z0bkU5aCUyQlc4ZGVIcWVEOVpWdHVIJTJCTEVsNExlRnN0SGR6TFE2QVVVbiUyRjZSVHRPaVlSSVV2WW1Rc0VDaDlzYlFYWjk0SGRkVnVnS1dHUm5PUVVlVWU0dUlBS3JNOUdOdiUyRm85OHYxbFk1YWZINVRRa1lWenN0UnRiellMbGJuQSUzRCUzRA",
            "innity.dmp.6.sess": "2.1736130231189.1736130231189.1736130246064",
            "_ga_3SG1F1VR0F": "GS1.2.1736130225.1.1.1736130246.0.0.0",
            "_ga": "GA1.2.1390962722.1736130225",
            "_ga_7JX55F9G2F": "GS1.2.1736130225.1.1.1736130275.10.0.0",
            "_ga_8XXQD60WTC": "GS1.2.1736130225.1.1.1736130275.10.0.0",
            "_ga_BQ281Q8KGX": "GS1.1.1736130225.1.1.1736130276.9.0.0",
            "_ga_GFV1BED9GW": "GS1.1.1736130225.1.1.1736130284.1.0.0"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json['stories']
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            # print(item)
            items = Item()
            items.article_url ="https://www.malaysiakini.com" + item.get("link")
            items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://www.malaysiakini.com/api/en/search/{current_page}"
        params = {
            "searchQuery": f"{current_keyword}",
            "queryCategory": "",
            "queryDateFrom": "2000-01-01",
            "queryDateTo": "2025-01-01",
            "sort": "desc"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@id='story_content_analytics']//p/text() | //div[@itemprop='articleBody']//p//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
