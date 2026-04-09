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

    country = 'Singapore'
    table = 'Singapore'
    #英语
    keywords = ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://www.businesstimes.com.sg/_plat/api/v1/queryly-search"
            params = {
                "query": f"{keyword}",
                "endindex": "0",
                "batchsize": "10",
                "sort": "relevancydate"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "newrelic": "eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjIyNTAyMyIsImFwIjoiMTEwMzMxMjUxNSIsImlkIjoiMTI3MmE4NGJiOTM0MWNhMiIsInRyIjoiOGYxNDM0NTQwZWExMGZiMjJlMWEwZDI3ZDA4NTcxMWQiLCJ0aSI6MTczNjQ2OTgzOTYxM319",
            "priority": "u=1, i",
            "referer": "https://www.businesstimes.com.sg/search",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "traceparent": "00-8f1434540ea10fb22e1a0d27d085711d-1272a84bb9341ca2-01",
            "tracestate": "225023@nr=0-1-225023-1103312515-1272a84bb9341ca2----1736469839613",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "suid": "ec0b2d1b828d4580ab1cfb7b8d1147e4",
            "_vwo_uuid_v2": "D96AD0F0A9E8F379CCBCC2A88D0DD8D6A|1102927b78406e1430688d58e89ad182",
            "neuronId": "aeafcab6-c1f5-4738-9744-9efee9847c7e",
            "_gcl_au": "1.1.753213598.1736469566",
            "mySPHUserType": "y-anoy",
            "visitorcat": "1",
            "sph_user_country": "SG",
            "spgwAMCookie": "r1o6v7lqik3g5kk69ghsjsbiem",
            "_cb": "ui3UpCsfBxNDZU7m5",
            "_cb_svref": "https%3A%2F%2Fdocs.qq.com%2F",
            "permutive-id": "c5a8ee6b-7b08-4a74-9c10-1b57283b7915",
            "_fbp": "fb.2.1736469566823.29012821927700263",
            "FPID": "FPID2.3.sKt%2BIHjc4X9x84Zxj0ILphnvp%2FIJNKQRWe7idpFq%2B6o%3D.1736469566",
            "FPLC": "UXEmRGRaup0xoGZp2GCKPWpVyGWR5WzIK6zpNa4FRK%2BtvyOx%2BQ0T%2FI73Mv0%2FHmhJa4rlrWk6sc6YNJa14Tn3RMPq4B1YYeuP6tPCUI%2BWkaNlPY56mJvQyZqCwGEhqA%3D%3D",
            "_gid": "GA1.3.262767134.1736469567",
            "sui_1pc": "1736469568990F89523DDC577794E2FDA78A38303914F8D14259C2B0",
            "dicbo_id": "%7B%22dicbo_fetch%22%3A1736469569601%7D",
            "BTPulseTooltip": "shown",
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "panoramaId": "3d6e8b78d1ec91ae6e29e218acf4185ca02c60046cf65798b2f03d4a5c35863c",
            "panoramaIdType": "panoDevice",
            "exit_intervention": "true",
            "FPGSID": "1.1736469556.1736469773.G-BDB8DV371E.XdUpAilywKx4E85-I3jwdg",
            "_ga_SM7K1EMZHH": "GS1.1.1736469571.1.1.1736469797.45.0.0",
            "_chartbeat2": ".1736469566664.1736469798422.1.6zBI4Dw4cscBE6nRVBnjtNTDfpGhF.6",
            "_chartbeat5": "",
            "_ga": "GA1.1.220121795.1736469566",
            "_ga_BDB8DV371E": "GS1.1.1736469565.1.1.1736469798.0.0.110976185",
            "AWSALB": "wb0BDASg9+5oJIPM2DfK8jmzhkYYBi18zFvRdK4gbPPbjb0kt7Y97LoQ1uvypjV8S3eei0dtIv9r039igf9zHMQwEtp8Sll+osnNQHEr+aAgupcKohSOJrDuRWA8",
            "AWSALBCORS": "wb0BDASg9+5oJIPM2DfK8jmzhkYYBi18zFvRdK4gbPPbjb0kt7Y97LoQ1uvypjV8S3eei0dtIv9r039igf9zHMQwEtp8Sll+osnNQHEr+aAgupcKohSOJrDuRWA8",
            "panoramaId_expiry": "1737074591246",
            "ph_phc_yQ78F4A3sjKgkDMBvEsc1dNOEbSJfiHomZduQs1YL7z_posthog": "%7B%22distinct_id%22%3A%2201944da6-cdfd-7d60-a3db-c8d39c980f93%22%2C%22%24sesid%22%3A%5B1736469839605%2C%2201944da6-cdfc-7e8a-896b-511c00bce5d5%22%2C1736469564924%5D%7D"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json["items"]
        # print(data)

        # print(links)
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
            # print(item.get("url"))
            items = Item()
            items.table_name = self.table
            items.article_url = item.get("link")
            items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 10
        url = "https://www.businesstimes.com.sg/_plat/api/v1/queryly-search"
        params = {
            "query": f"{current_keyword}",
            "endindex": f"{current_page}",
            "batchsize": "10",
            "sort": "relevancydate"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        # items.title = response.xpath("//h1/text()").extract_first()
        content = "".join(response.xpath("//p[@data-story-element='paragraph']/text()").extract())
        items.content = content
        items.author = ''
        items.pubtime = response.xpath("//meta[@name='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
