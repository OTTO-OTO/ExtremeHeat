import json
from keyword import kwlist

import feapder
from feapder import Item


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[5, 8],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB="importance_country_data",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        # ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )

    country = 'China'
    keywords = ["高温", "高温增加", "高温影响", "温度上升", "热事件", "温度升高", "强降雨", "强降水", "暴雨", "极端降雨", "严重干旱", "长期干旱", "水资源短缺", "停电", "高温停电", "热浪停电", "高温导致停电", "火灾", "高温火灾", "热火灾", "温度火灾"]
    table = 'China'

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "http://search.people.cn/search-platform/front/search"
            data = {
                "key": f"{keyword}",
                "page": 1,
                "limit": 10,
                "hasTitle": True,
                "hasContent": True,
                "isFuzzy": True,
                "type": 1,
                "sortType": 2,
                "startTime": 0,
                "endTime": 0
            }
            data = json.dumps(data, separators=(',', ':'))
            yield feapder.Request(url, data=data, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Content-Type": "application/json;charset=UTF-8",
            "Origin": "http://search.people.cn",
            "Referer": "http://search.people.cn/s?keyword=%E7%8F%8A%E7%91%9A%E7%99%BD%E5%8C%96&st=1&_=1736211378885",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "__jsluid_h": "aa21656e4c77ef9be93704ec53f95359",
            "sso_c": "0",
            "sfr": "1"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        if request.page >= 10:
            print(f"关键词 {current_keyword} 的第 {request.page} 页已经爬取完毕，退出当前关键字的循环")
            return None
        links = response.json.get("data").get("records")
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            print(item.get("url"))
            items = Item()
            items.article_url = item.get("url")
            items.country = self.country
            items.title = item.get("title")
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items, page=request.page)

        current_page = request.page + 1
        url = "http://search.people.cn/search-platform/front/search"
        data = {
            "key": f"{current_keyword}",
            "page": current_page,
            "limit": 10,
            "hasTitle": True,
            "hasContent": True,
            "isFuzzy": True,
            "type": 1,
            "sortType": 2,
            "startTime": 0,
            "endTime": 0
        }
        data = json.dumps(data, separators=(',', ':'))
        yield feapder.Request(url, data=data, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        # items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath(
            "//div[contains(@class, 'box_con') or contains(@class, 'rm_txt_con cf')]//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@name='publishdate']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
