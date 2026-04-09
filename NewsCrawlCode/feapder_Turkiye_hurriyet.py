# -*- coding: utf-8 -*-
# 173
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

    country = 'Türkiye'
    table = 'Türkiye'
    #土耳其语
    keywords = ["Tropikal siklon", "Tropikal depresyon", "Tropikal fırtına", "Tayfun", "Kasırga", "Siklon", "Fırtına", "Yoğun yağmur", "Sel", "Dalga", "Kıyı hasarı", "Kayma", "Jeolojik felaket", "Deniz felaketi", "Güçlü rüzgarlar", "Tayfun felaketi", "Toprak kayması", "Yer kayması"]


    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://www.hurriyet.com.tr/api/search/searchcontentloadmore"
            params = {
                "query": f"{keyword}",
                "page": "1",
                "isFromNewsSearchPage": "false"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=1, i",
            "referer": "https://www.hurriyet.com.tr/haberleri/ekstrem",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "hcatFrom": "anasayfa",
            "_gcl_au": "1.1.1798026652.1736734764",
            "_ga": "GA1.1.66301789.1736734764",
            "isWebSiteFirstVisit": "true",
            "_sksid": "5cb76d6446624d488cf670f9436176cb",
            "_skulp": "2025%2F1%2F13",
            "_skou": "direct",
            "_skouu": "https%3A%2F%2Fwww.hurriyet.com.tr%2F",
            "_sksl": "%5B%22_sksid%22%2C%22js_skinit_id%22%2C%22_skou%22%2C%22_skouu%22%5D",
            "_skrc": "5cb76d6446624d488cf670f9436176cb",
            "_skbid": "e6d0b472d22242a18b99055120b44fb0",
            "_ym_uid": "1736734766945238891",
            "_ym_d": "1736734766",
            "_ym_isad": "2",
            "ASP.NET_SessionId": "0whqcaf0mbsi1uju2xiiqxdr",
            "js_skinit_id": "26394f4516d14fe9b5d1459a5151b2e7",
            "_ga_6RJDJJ7YKC": "GS1.1.1736734763.1.1.1736734817.6.0.0",
            "isPopupShow": "true"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//div[@class='tag__list__item']/a/@href").extract()
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
        url = "https://www.hurriyet.com.tr/api/search/searchcontentloadmore"
        params = {
            "query": f"{current_keyword}",
            "page": f"{current_page}",
            "isFromNewsSearchPage": "false"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        content = "".join(response.xpath("//div[@class='news-content readingTime']//p/text()").extract())
        items.content = content
        items.author = ''
        items.pubtime = response.xpath("//meta[@name='datePublished']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
