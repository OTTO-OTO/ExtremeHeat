import feapder
from feapder import Item
from lxml import etree
import re


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[5, 10],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB="other_country_site1",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    country = 'Libya'
    table = 'Libya_libyaobserver'
    keywords =[ "Extreme heatwave","Extreme temperature", "Heatwave event", "Increase in high temperature", "High temperature impact", "High temperature", "Strong heat", "Temperature rise", "Heat event", "Temperature increase", "Heavy rainfall", "Heavy precipitation", "Torrential rain", "Extreme rainfall", "Drought", "Severe drought", "Long-term drought", "Water shortage", "Power outage", "High temperature power outage", "Heatwave power outage", "Power outage caused by high temperature", "Fire", "High temperature fire", "Heat fire", "Temperature fire", "Heat-induced fire", "Agricultural impact", "Heatwave agriculture", "Crop damage", "Agricultural heat stress", "Hypoxia", "Heatstroke", "Heat-induced heatstroke", "High temperature hypoxia", "High temperature heatstroke", "Traffic impact", "High temperature traffic", "Heatwave traffic", "Temperature traffic", "Ecological disaster", "Heat disaster", "High temperature environment", "Impact of heat on biodiversity", "Heatwave ecology", "Pollution", "High temperature pollution", "Heat pollution", "Temperature pollution", "Coral bleaching", "High temperature coral reefs", "Temperature coral bleaching" ]
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://libyaobserver.ly/search/node"
            params = {
                "keys": f"{keyword}",
                "page": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://libyaobserver.ly/search/node?keys=rain",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-arch": "\"x86\"",
            "sec-ch-ua-bitness": "\"64\"",
            "sec-ch-ua-full-version": "\"131.0.2903.112\"",
            "sec-ch-ua-full-version-list": "\"Microsoft Edge\";v=\"131.0.2903.112\", \"Chromium\";v=\"131.0.6778.205\", \"Not_A Brand\";v=\"24.0.0.0\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-model": "\"\"",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-ch-ua-platform-version": "\"10.0.0\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "_ga": "GA1.1.921091325.1735976203",
            "cf_clearance": "_9jgCtVUqLZ1rriShM5goS45tqEDHzEh6FovR2TnHlQ-1735976194-1.2.1.1-Y7TB2JlH.ZnCDE3HFeCXCEvw2FIBUL8mYudM1LubQrmlQaBJ99ta4tmpeJE4hOMTaXoR3rTETy5Ri9WrHcoauJFTm8HNMXOvbLhpHB2pqwRNqM2_29Lgb2GNgGnY5xWHxk1yem9_68GrgEDULhgMaDxBQJZIse4NmeUzULUTjEfYxtrB2e.7SKTYLILE07PyQOivwZ7IkEr64GLNaud2SbWyLKkzG5SLEnd_fpG0ipp.Y3f1Gb9XqYeCHup2g5qG00PAfNO30AkotIpVwyha53dKvr49hw37iNAtS3TZu3QzDdYhnpQMlOLFycVPWgzlYmq1LvmKYcwYn6LRowMMpppbagx_KcaRvgx_6Kli3RZu0CaDw1q3MTwAu8l1PaXB1h5BX.oyquk_4E29lzvEeQrRQdb7GGqDaMzB4cEBT2h3t98LBdX8gomStpjv6hOClCjpLN1zr9.g81RyGZB7eA",
            "__gads": "ID=4b5331221c94aab2:T=1735976195:RT=1735976195:S=ALNI_MZQhFAbRpSKbM0U19_t_S_9LIPD6w",
            "__gpi": "UID=00000fd41ff9d6d8:T=1735976195:RT=1735976195:S=ALNI_MbNgDdra31tWH7LHc4m2T7jlkWg0A",
            "__eoi": "ID=600c0b88bb3eb286:T=1735976195:RT=1735976195:S=AA-AfjahZ-37pXAIqO3oE_Y5iHzT",
            "FCNEC": "%5B%5B%22AKsRol-syUixryBawZCQunSMmHf6eJ3-WMouy7piemBalTTVNoFk9l7IuW6bQNiomz6kmbA1wwA_WRYYrwExIut_7AdWwGP3KWKhkpIc1Z3sog0PnBPy0OJT1NNVUuPNAdwkl9K9r7T-Quk-qZ44tNiyWnBuYiI6Yg%3D%3D%22%5D%5D",
            "_ga_HE30Z4PC4X": "GS1.1.1735976202.1.1.1735976258.4.0.0"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//h3/a/@href").extract()
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
        url = "https://libyaobserver.ly/search/node"
        params = {
            "keys": f"{current_keyword}",
            "page": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@data-role='article_content']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time[@class='entry-date published updated']/span/text()").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
