import feapder
from NewsItems import SpiderDataItem
from feapder import Item

class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[5, 8],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB="TropicalCyclone",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING = dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )

    )

    country = 'China'
    keywords = ["热带气旋", "热带低气压", "热带风暴", "台风", "飓风", "气旋", "风暴", "暴雨", "洪水", "风暴潮", "沿海灾害", "滑坡", "地质灾害", "海洋灾害", "强风", "台风灾害", "泥石流", "山体滑坡"]

    table = 'China'

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://so.news.cn/getNews"
            params = {
                "lang": "cn",
                "curPage": "1",
                "searchFields": "0",
                "sortField": "0",
                "keyword": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Referer": "https://so.news.cn/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        request.cookies = {
            "wdcid": "0e3f19f2c78a22c9",
            "acw_tc": "1a0c652317362107920453048e013b7e9900aec8f30d2a63217c39261ca1d9",
            "org.springframework.web.servlet.i18n.CookieLocaleResolver.LOCALE": "zh_CN"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json["content"].get("results")
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        if request.page >= 10:
            print(f"关键词 {current_keyword} 的第 {request.page} 页已经抓取完毕，退出当前关键字的循环")
            return None

        for item in links:
            print(item.get("url"))
            items = Item()
            items.article_url = item.get("url")
            items.country = self.country
            items.title = item.get("title")
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items,page=request.page)

        current_page = request.page + 1
        url = "https://so.news.cn/getNews"
        params = {
            "lang": "cn",
            "curPage": f"{current_page}",
            "searchFields": "0",
            "sortField": "0",
            "keyword": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        # items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@id='detail']//p//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@name='publishdate']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
