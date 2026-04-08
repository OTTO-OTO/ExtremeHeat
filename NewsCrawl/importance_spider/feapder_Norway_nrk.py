import feapder
from feapder import Request, Response

from NewsItems import SpiderDataItem

class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[8, 10],
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

    country = 'Norway'
    #挪威语
    keywords =["Syklon i tropene", "Tropisk depresjon", "Tropisk storm", "Tyfon", "Orkan", "Syklon", "Storm", "Kraftig regn", "Flom", "Sjøsprøyt", "Kystskade", "Skred", "Geologisk katastrofe", "Maritim katastrofe", "Kraftige vinder", "Tyfonkatastrofe", "Jordskred", "Landskred"]
    table = 'Norway'

    def start_requests(self):
        for keyword in self.keywords:
            page = 1  # 将page变量作为实例变量
            url = "https://www.nrk.no/sok/"
            params = {
                "q": f"{keyword}",
                "scope": "nrkno",
                "from":"10"  # 从第1个结果开始，每次增加10
            }
            yield feapder.Request(url, params=params, callback=self.parse_url,page=page,keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.nrk.no/sok/?q=Varm%20&scope=nrkno",
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
            "_nrkbucket1": "2",
            "i00": "0000676d0969f1550000",
            "_ga": "GA1.2.1930908339.1735199081",
            "sp": "eb73266a-50fe-486e-8845-bf89e66374a0",
            "nrk-login-context-info": "%7B%22clientId%22%3A%22nrk.no.web%22%2C%22authority%22%3A%22https%3A%2F%2Finnlogging.nrk.no%22%7D",
            "nrk-login-session-state": "none",
            "_gid": "GA1.2.1046807041.1735287392",
            "_sp_ses.eecb": "*",
            "_gat_ca_kurator": "1",
            "__mbl": "75@{\"u\":[{\"uid\":\"zgxZuwZDlTMeFLNM\",\"c\":\"desktop\",\"ts\":1735287534},1735377534]}",
            "_gat_ca_nrknoserum": "1",
            "_k5a": "61@{\"u\":[{\"uid\":\"qC7oaRKZD5BfvrPu\",\"ts\":1735287535},1735377535]}",
            "_sp_id.eecb": "095c1a4c-44c5-4778-a379-5a7d873dfeaf.1735199080.2.1735287544.1735199664.7cf4f87e-87da-489c-9c3d-54dac57764fc.e8903267-42e6-4ba8-8d4f-947e3fb613fd.15e16007-9c6b-4d3b-8193-3a2aec735853.1735287392616.19"
        }
        return request


    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//a[@class='nrkno-search-hit__link']/@href").extract()
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            items = SpiderDataItem()
            items.article_url = item
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.nrk.no/sok/"
        params = {
            "q": f"{current_keyword}",
            "scope": "nrkno",
            "from": f"{current_page * 10}"  # 从第1个结果开始，每次增加10
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        # print(response.text)
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@data-ec-list='article_body']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items

if __name__ == "__main__":
    AirSpiderDemo().start()