import feapder
from feapder import Item
from lxml import etree


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
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
    country = 'Bulgaria'
    table = 'Bulgaria_nova'
    keywords = [
        'Екстремно', 'Горещина', 'Висока температура', 'Силен дожд', 'Суша',
        'Изключване на енергията поради горещина', 'Пожар', 'Въздухна загрязнение', 'Климатичен промен',
        'Намаляване на сетъците', 'Оксигенно недостиг', 'Високата температура влияе на трафика',
        'Екологична катастрофа', 'Климатичните промени влияят на икономиката', 'Морска горещина',
        'Загрязнение от висока температура', 'Корал'
    ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://nova.bg/search/news/1/"
            params = {
                "q": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://nova.bg/search/news?q=%D0%95%D0%BA%D1%81%D1%82%D1%80%D0%B5%D0%BC%D0%BD%D0%BE",
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
            "cx_test": "",
            "device": "1",
            "_ga": "GA1.1.1923292567.1735784967",
            "cx_id": "6775fa041bdfbf0dc3ca1957",
            "cx_last_match": "1735784972730",
            "__io_d": "1_705468254",
            "__io_lv": "1735784973083",
            "__io": "42d83bf85.63e351b2e_1735784973083",
            "__io_session_id": "18c88273e.67a00e391_1735784973083",
            "__io_nav_state33821": "%7B%22current%22%3A%22%2F%22%2C%22currentDomain%22%3A%22nova.bg%22%2C%22previousDomain%22%3A%22%22%7D",
            "__io_unique_33821": "2",
            "__io_uh": "1",
            "__io_visit_33821": "1",
            "__gfp_64b": "Yg27C7GoEHXXIUsHA7AixByBq8do4w0ZaO1XeFTrSDP.R7|1735784972|2|||8:1:80",
            "_ga_79JNJVS3T9": "GS1.1.1735784966.1.1.1735785024.2.0.0"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//a[@class='title']/@href").extract()
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
        url = f"https://nova.bg/search/news/{current_page}/"
        params = {
            "q": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@itemprop='description']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@itemprop='datePublished']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
