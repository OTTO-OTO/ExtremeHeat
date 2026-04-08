# -*- coding: utf-8 -*-

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

    country = 'Switzerland'
    table = 'Switzerland'
    #法语
    keywords = ["Cyclone tropical", "Dépression tropicale", "Tempête tropicale", "Typhon", "Ouragan", "Cyclone", "Tempête", "Pluies diluviennes", "Inondation", "Houle", "Dommages côtiers", "Glissement", "Catastrophe géologique", "Catastrophe marine", "Vents violents", "Catastrophe de typhon", "Glissement de terrain", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.letemps.ch/articles"
            params = {
                "button": "",
                "page": "1",
                "query": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.letemps.ch/articles?query=Temp%C3%A9rature+extr%C3%AAme&button=",
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
            "anonymous_id": "IjAxOTQ0ZWJmLTkzOGUtNzcyYS04MjEzLTUzNWY0NWViMTkwNSI%3D--524f7b927f9c07c29304de87f40e6550be16980b",
            "SRVGROUP": "common",
            "anonymous_visit_count": "1",
            "didomi_token": "eyJ1c2VyX2lkIjoiMTk0NGViZmMtYjNjNS02NzliLWI0ZTYtZDU2OGE5NWM1NjJmIiwiY3JlYXRlZCI6IjIwMjUtMDEtMTBUMDU6NDY6MTkuODM2WiIsInVwZGF0ZWQiOiIyMDI1LTAxLTEwVDA1OjQ2OjIxLjkwNVoiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwiYW1hem9uIiwidHdpdHRlciIsImM6dGFwYWRpbmMtVkRUVVVjS3ciLCJjOm1ldGEtNDNIRG1GUmEiLCJjOnBhcnNlbHluZS1wZmRnMk1BUCIsImM6eW91dHViZSIsImM6aG90amFyIiwiYzpjaGFydGJlYXQiLCJjOmxpbmtlZGluIiwiYzp0aWt0b2stS1pBVVFMWjkiLCJjOmdvb2dsZWFuYS00VFhuSmlnUiIsImM6YXdzLWNsb3VkZnJvbnQiLCJjOnBpYW5vaHlici1SM1ZLQzJyNCIsImM6dW5ydWx5Z3JvLW5LeUxxZEtpIiwiYzpmb3JlY2FzdC1aUkpQQVVUNyIsImM6c3B5cmktWEF0QlB4M2kiLCJjOmp3LXBsYXllciIsImM6bWljcm9zb2Z0IiwiYzplY2hvYm94LUdxRDk2TDlLIiwiYzptYWduaXRlaW4tZVBFcGJDUTciLCJjOm5vbmxpLWpuV2Y4a3FEIl19LCJwdXJwb3NlcyI6eyJlbmFibGVkIjpbImdlb2xvY2F0aW9uX2RhdGEiLCJkZXZpY2VfY2hhcmFjdGVyaXN0aWNzIiwiYXVkaWVuY2VtLWhKeGFlR3JSIl19LCJ2ZXJzaW9uIjoyLCJhYyI6IkNqQ0FLQUZrQ0pJSWlnUlNnb3dBLkFBQUEifQ==",
            "euconsent-v2": "CQLA94AQLA94AAHABBENBXFsAP_gAAAAAAAAIzJB5C5USCFBIGJmYMkAYAQFxxgBQEAAAAABAoAAABKAIAQAAEAAIAAAAAAAAAAAAAAAAABAEAAAAAAAIIAAIAAACAAAAAAIAAAAAAAAQgAAAAAAAAAAAAAAAAAAAAAAgAAACBIAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAYAAAEAAAIAAAAAAAABAAAAMSgAwABBV8pABgACCr46ADAAEFXyEAGAAIKvhIAMAAQVfLQAYAAgq-.f_wAAAAAAAAA",
            "_gcl_au": "1.1.2007983566.1736487982",
            "_ga": "GA1.1.152697444.1736487982",
            "_parsely_session": "{%22sid%22:1%2C%22surl%22:%22https://www.letemps.ch/%22%2C%22sref%22:%22https://docs.qq.com/%22%2C%22sts%22:1736487982957%2C%22slts%22:0}",
            "_parsely_visitor": "{%22id%22:%22pid=ef6dd7fc-657c-40a9-9e50-54cc950dda81%22%2C%22session_count%22:1%2C%22last_session_ts%22:1736487982957}",
            "_pprv": "eyJjb25zZW50Ijp7IjAiOnsibW9kZSI6Im9wdC1pbiJ9LCI3Ijp7Im1vZGUiOiJvcHQtaW4ifX0sInB1cnBvc2VzIjp7IjAiOiJBTSIsIjciOiJETCJ9LCJfdCI6Im1sZXIxZzlxfG01cWMzeXhxIn0%3D",
            "_pcid": "%7B%22browserId%22%3A%22m5qc3yxo24gdezop%22%2C%22_t%22%3A%22mler1g9s%7Cm5qc3yxs%22%7D",
            "_pctx": "%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAE0RXSwH18yBbVLACMAcwCcEAD78ArAEcAxgGYAngA94AXyA",
            "_tt_enable_cookie": "1",
            "_ttp": "VBemtAP33CH_LBYHd1FQBYxvYLg.tt.1",
            "_fbp": "fb.1.1736487983348.850935970540178414",
            "nli": "70d01a2c-221d-a84d-ef34-6fe67e5e049e",
            "_hjSession_3552646": "eyJpZCI6ImJjZmU4MzZjLWQ2ZDgtNDIzNC05ZTgzLWVhNWU2Njg1OGI1ZiIsImMiOjE3MzY0ODc5ODg0NTMsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MH0=",
            "_heidi_session": "N2STMuGOKgzZsUe4XaKJ5m4pg1HBUwv1hVOiQJlYwxIhfdCUvXtwt6wd3VorFb%2F7812gHnvJcWdqSxPBjfnHkO9eCy11QK5ZV3oDM7wFuIEEuAojkT3MUAoawRZI3f8GmCwUOjbPHrI2IwwEB6%2B62CrajcFRSXnN5JbxepfYat9x9Q%3D%3D--xd0rZtNJM2hcPZ60--%2BvC21w657tSuerV2cgxO4g%3D%3D",
            "_ga_SR8FQJPVG5": "GS1.1.1736487979.1.1.1736488067.60.0.0",
            "_hjSessionUser_3552646": "eyJpZCI6IjEyOGE1ZjZlLWEzMmEtNTM2My1iZGZlLTFjOTg0ZmVjNDczNyIsImNyZWF0ZWQiOjE3MzY0ODc5ODg0NTIsImV4aXN0aW5nIjp0cnVlfQ==",
            "_parsely_slot_click": "{%22url%22:%22https://www.letemps.ch/articles?query=Temp%25C3%25A9rature+extr%25C3%25AAme&button=%22%2C%22x%22:395%2C%22y%22:5858%2C%22xpath%22:%22//body/main[1]/div[1]/div[1]/nav[2]/span[2]/a[1]%22%2C%22href%22:%22https://www.letemps.ch/articles?button=&page=2&query=Temp%25C3%25A9rature+extr%25C3%25AAme%22}"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//h2[@class='post__title']/a/@href").extract()
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
            # print(item.get("url"))
            items = Item()
            items.table_name = self.table
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.letemps.ch/articles"
        params = {
            "button": "",
            "page": f"{current_page}",
            "query": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        content = "".join(response.xpath("//article//p/text()").extract())
        items.content = content
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='og:article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
