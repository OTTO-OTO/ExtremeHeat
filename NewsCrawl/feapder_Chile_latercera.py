import feapder
from feapder import Item
from lxml import etree
import re


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=3,  # 爬虫并发数，追求速度推荐32
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
    country = 'Chile'
    table = 'Chile_latercera'
    keywords = [
        'Extremo', 'Calor', 'Alta Temperatura', 'Lluvia Intensa', 'Sequía',
        'Apagón por Calor', 'Incendio', 'Contaminación del Aire', 'Cambio Climático',
        'Reducción de la Cosechas', 'Deficiencia de Oxígeno', 'Alta Temperatura Afectando el Tráfico',
        'Desastre Ecológico', 'Cambio Climático Afectando la Economía', 'Ola de Calor Marina',
        'Contaminación por Alta Temperatura', 'Coral'
    ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://cse.google.com/cse/element/v1"
            params = {
                "rsz": "filtered_cse",
                "num": "10",
                "hl": "en",
                "source": "gcsc",
                "start": "0",
                "cselibv": "5c8d58cbdc1332a7",
                "cx": "0099769ad8b6d3d5e",
                "q": f"{keyword}",
                "safe": "off",
                "cse_tok": "AB-tC_6tkIU5YanKi1t7zqFG9vlB:1740098794481",
                "exp": "cc,apo",
                "callback": "google.search.cse.api18092",
                "rurl": f"https://www.latercera.com/search/?q={keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Referer": "https://journaldutchad.com/?s=fortes+pluies",
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
            "_ga": "GA1.1.2112182503.1734576158",
            "__gads": "ID=129b16d63327ce56:T=1734576171:RT=1735788288:S=ALNI_Mb4l8vCBIdAw2A9ZC2l0oPq9jU17g",
            "__gpi": "UID=00000fab1c64c0a0:T=1734576171:RT=1735788288:S=ALNI_MZxRjzX7rAT0zGzQ7Aebf_U4GrvXw",
            "__eoi": "ID=4a9cd62070e720e7:T=1734576171:RT=1735788288:S=AA-AfjatJfRAuVQQWIM4-d_Bz38m",
            "_ga_NP6HQYZH23": "GS1.1.1735788285.2.1.1735788410.0.0.0",
            "FCNEC": "%5B%5B%22AKsRol8excEocicNqrddzzW3Y6SmDZUqzIm0LBQ7qqe7y0C__-C3-WvnL3-ria7D5ZICeJXeMr8szF_XgbqTobaRYIGKosv4kfP8Mge_bEmSofyYg71XgGOgKTMByc6OdaRLoz40k1-Y_gymCWiRLHVHhwBGtgtVIQ%3D%3D%22%5D%5D"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        data = response.text.split("api18092(")[-1].split(");")[0]
        pattern = r'"url": "(.*?)"'
        links = re.findall(pattern, data)
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links[:10]:
            print(item)
            items = Item()
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://cse.google.com/cse/element/v1"
        params = {
            "rsz": "filtered_cse",
            "num": "10",
            "hl": "en",
            "source": "gcsc",
            "start": f"{current_page}",
            "cselibv": "5c8d58cbdc1332a7",
            "cx": "0099769ad8b6d3d5e",
            "q": f"{current_keyword}",
            "safe": "off",
            "cse_tok": "AB-tC_6tkIU5YanKi1t7zqFG9vlB:1740098794481",
            "exp": "cc,apo",
            "callback": "google.search.cse.api18092",
            "rurl": f"https://www.latercera.com/search/?q={current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1//text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='single-content']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
