import feapder
from NewsItems import SpiderDataItem
from lxml import etree

class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=3,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[6, 10],
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
    country = 'Belarus'
    table = 'Belarus_belta'
    keywords = [
    'жара',
    'высокая температура',
    'сильный дождь',
    'засуха',
    'отключение электроснабжения из-за жары',
    'пожар',
    'загрязнение воздуха',
    'изменение климата',
    'снижение урожайности',
    'недостаток кислорода',
    'высокая температура влияет на движение',
    'экологическая катастрофа',
    'изменение климата влияет на экономику',
    'мареinal heatwave',
    'загрязнение высокими температурами',
    'кораллы'
]
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://belta.by/search/getResults/page/1/"
            params = {
                "query": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page,keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Referer": "https://belta.by/search/getResultsForPeriod/page/2/?query=%D0%B6%D0%B0%D1%80%D0%B0&group=2&period=0",
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
            "_ym_uid": "1734486487283317423",
            "_ym_d": "1734486487",
            "f_version": "",
            "ADC_CONN_539B3595F4E": "CE07EA272864829299AFD7A496F1130A68D887CE8309FD9D341C498525E7B0CBCD06D42A76F40917",
            "ADC_REQ_2E94AF76E7": "62753B64E39FE6CEA227B7885EEBA54DC51B4CF4BC1F8AD66D0E0CCFEBD7F310A5C97D183654BEB2",
            "_ym_visorc": "b",
            "_ym_isad": "2",
            "PHPSESSID": "bffjfevmpchop9o223hlac8al7"
        }
        return request


    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//div[@class='rubric_item search_item']/div/a/@href").extract()
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            items = SpiderDataItem()
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)


        current_page = request.page + 1
        url = f"https://belta.by/search/getResults/page/{current_page}/"
        params = {
            "query": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//meta[@property='og:title']/@content").extract_first()
        items.content = "".join(response.xpath("//div[@class='js-mediator-article']/text()").extract()).replace('\n','').replace('\r','')
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items

if __name__ == "__main__":
    AirSpiderDemo().start()
