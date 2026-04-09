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
    country = 'Mali'
    table = 'Mali_maliweb'
    keywords =  [
    "extrême chaleur",
    "températures élevées",
    "fortes pluies",
    "sécheresse",
    "panne d'électricité due à la chaleur",
    "incendie",
    "pollution de l'air",
    "changement climatique",
    "réduction des rendements agricoles",
    "hypoxie",
    "coup de chaleur",
    "impact de la chaleur sur le trafic",
    "désastre écologique",
    "impact du changement climatique sur l'économie",
    "vague de chaleur marine",
    "pollution liée à la chaleur",
    "corail"
]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.maliweb.net/page/1"
            params = {
                "s": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Referer": "https://www.maliweb.net/?s=fortes+pluies",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        request.cookies = {
            "_gid": "GA1.2.2066512491.1736131598",
            "_ga_Q8MZ0VB7T2": "GS1.1.1736131598.1.1.1736131926.0.0.0",
            "_ga": "GA1.2.1543399694.1736131598",
            "_gat": "1",
            "_ga_GQWL0D0V0J": "GS1.2.1736131598.1.1.1736131926.60.0.0",
            "__gads": "ID=b9b4e08bd05e13b8:T=1736131589:RT=1736131916:S=ALNI_MY2tNaIZFq6yoS5xJXC9oEdugi_DA",
            "__gpi": "UID=00000fd62d02d418:T=1736131589:RT=1736131916:S=ALNI_MZ34DO1Ym8JhUVrj5KnkziM_3y-jw",
            "__eoi": "ID=5d7e744e04be6ce8:T=1736131590:RT=1736131916:S=AA-AfjbO5CVgJBvtjvG92imxhL4w",
            "FCNEC": "%5B%5B%22AKsRol-GPQx5NNKblFypy5x6AnFMD6orVPv8vAtZ4FmdRfZEDwuEN-QTymcpL5-mxnJRA8dgnhOEVr2ygWU1Oo4TGVHldfghlI3e2_0yByHh_2NX9sXcgZzNGRt4WuEng3-EsdeiPoSUHaBBGj5IKGU6Inz5jOFfwQ%3D%3D%22%5D%5D"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//div[@class='td-pb-row']//h3/a/@href").extract()
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
        url = f"https://www.maliweb.net/page/{current_page}"
        params = {
            "s": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='td-post-content tagdiv-type']//p//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
