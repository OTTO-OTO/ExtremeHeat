# -*- coding: utf-8 -*-
# 173
import feapder
from feapder import Item
import json
from lxml import etree


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

    country = 'Uruguay'
    table = 'Uruguay'
    #西班牙语
    keywords = ["Ciclón tropical", "Depresión tropical", "Tormenta tropical", "Tifón", "Huracán", "Ciclón", "Tormenta", "Lluvias intensas", "Inundación", "Marea", "Daños costeros", "Deslizamiento", "Desastre geológico", "Desastre marino", "Vientos fuertes", "Desastre de tifones", "Derrumbes de lodo", "Deslizamiento de tierra"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.elpais.com.uy/search"
            params = {
                "q": f"{keyword}",
                "p": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.elpais.com.uy/search?q=Temperatura+extrema",
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
            "cf_clearance": "quiCMQZ_46VIjTvTuUZoTPoy4JxcRPYfyqdoUkeDcXc-1736739714-1.2.1.1-g5wBM1f1GQ0YKqonOSXDVHuAPHYjNuwtkotNefVoa_wtQ3VHivZuKUtlKKuigKjZTPqQdtO2R6ySAdxEEO1dELntR6j3C2.GQ_iQTXUZqZUBVwZ.yklsGPiVXzEMmZ0GEfntRixVvYhFYQRXd56YeBJWQCZ8YkqNdwG6jIGCaoRnpASplwBvjWRfdKZz6raMqdnAyoMipbQx8mOxbexpAVuGLHRG2dwRCem6R9gY1ZbKy_eHWckbkLpgpi_M3j73alaOlmm.YtWhoCl9e2LKiOaFXKzVkL7lgJwewP1oRNh5w2xf5iajetCNKSuisNyxdDG2VvcEYXIbKEW4A.ha6w",
            "compass_uid": "75c4896b-a66b-419f-8a8a-ffc824507c41",
            "___nrbi": "%7B%22firstVisit%22%3A1736739736%2C%22userId%22%3A%2275c4896b-a66b-419f-8a8a-ffc824507c41%22%2C%22userVars%22%3A%5B%5B%22mrfExperiment_newRecTest%22%2C%224%22%5D%5D%2C%22futurePreviousVisit%22%3A1736739736%2C%22timesVisited%22%3A1%7D",
            "_gid": "GA1.3.1157769822.1736739749",
            "_pk_ses.KL91ITUDA0MTF.d199": "1",
            "_fbp": "fb.2.1736739750453.719355279308191451",
            "_ifdv": "bdc43035-9bc9-44d6-b51f-eaa3243827f1",
            "_if_location": "{\"country\":\"SG\",\"telco\":\"Digital Ocean\",\"city\":\"Singapore\"}",
            "_ifpv": "bdc43035-9bc9-44d6-b51f-eaa3243827f1",
            "_vfa": "www%2Eelpais%2Ecom%2Euy.00000000-0000-4000-8000-24109e8ba54f.22983406-6c9f-4f53-9c8e-5bbacb69108c.1736739750.1736739750.1736739750.1",
            "_vfz": "www%2Eelpais%2Ecom%2Euy.00000000-0000-4000-8000-24109e8ba54f.1736739750.1.medium=direct|source=|sharer_uuid=|terms=",
            "_pk_id.KL91ITUDA0MTF.d199": "46b821d655d34c89.1736739750.1.1736739841.1736739750.",
            "_pk_ref.KL91ITUDA0MTF.d199": "%5B%22%22%2C%22%22%2C0%2C%22%22%5D",
            "_gat": "1",
            "_gat_redElPaisTrk": "1",
            "___nrbic": "%7B%22isNewUser%22%3Atrue%2C%22previousVisit%22%3A1736739736%2C%22currentVisitStarted%22%3A1736739736%2C%22sessionId%22%3A%22ef681865-9e53-45c5-8614-543b8486660e%22%2C%22sessionVars%22%3A%5B%5D%2C%22visitedInThisSession%22%3Atrue%2C%22pagesViewed%22%3A2%2C%22landingPage%22%3A%22https%3A//www.elpais.com.uy/%22%2C%22referrer%22%3A%22https%3A//www.elpais.com.uy/%3F__cf_chl_tk%3DZ.gESXOO1zwtqCvqZiNWm9QSR9Xe59sTVwAu9HFJ5kc-1736739714-1.0.1.1-BeexFC6PRNN5FRQ7BrOZdLxgtnJ8GODBIGhps1WZDFQ%22%2C%22lpti%22%3Anull%7D",
            "_ga_HV7M7JL3QM": "GS1.1.1736739750.1.1.1736739841.58.0.0",
            "_ga": "GA1.1.1448183814.1736739738",
            "_ga_S7VNV656TN": "GS1.3.1736739750.1.1.1736739841.60.0.0",
            "_ga_CBSZFBWGVJ": "GS1.3.1736739750.1.1.1736739841.60.0.0",
            "_vfb": "www%2Eelpais%2Ecom%2Euy.00000000-0000-4000-8000-24109e8ba54f.8.10.1736739750.true...",
            "_ga_C3K9LY9JC6": "GS1.1.1736739738.1.1.1736739852.47.0.0"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//h2[@class='Promo-title']/a/@href").extract()
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
        url = "https://www.elpais.com.uy/search"
        params = {
            "q": f"{current_keyword}",
            "p": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1//text()").extract_first()
        content = "".join(response.xpath("//div[@class='Page-articleBody']//p/text()").extract())
        items.content = content
        items.author = ''
        items.pubtime = ''
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
