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

    country = 'Viet Nam'
    table = 'Viet_Nam'
    #越南语
    keywords = ["Bão nhiệt đới", "Áp thấp nhiệt đới", "Bão nhiệt đới", "Bão", "Bão lụt", "Vòi sen", "Bão", "Mưa lớn", "Lũ lụt", "Sóng biển", "Thiệt hại ven biển", "Sạt lở", "Thảm họa địa chất", "Thảm họa biển", "Cơn gió mạnh", "Thảm họa bão", "Lở đất", "Lở đất"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = f"https://thanhnien.vn/timelinesearch/{keyword}/1.htm"
            params = {
                "author": "0",
                "time": "0",
                "zone": "0",
                "type": "1",
                "sort": "0"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=1, i",
            "referer": "https://thanhnien.vn/tim-kiem.htm?keywords=nhi%E1%BB%87t%20%C4%91%E1%BB%99%20cao",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "x-requested-with": "XMLHttpRequest"
        }
        request.cookies = {
            "AviviD_uuid": "b5cee453-7fb7-4e1e-8054-2bfec26ab2e4",
            "webuserid": "6358d596-032a-b8f0-e8bb-0ede44f890b8",
            "_gcl_au": "1.1.1523845953.1736749687",
            "_io_ht_r": "1",
            "__io_d": "1_705468254",
            "__io_lv": "1736749687143",
            "__io": "dc8d3ecdf.736afab27_1736749687143",
            "__io_session_id": "e3858898c.a9ae06728_1736749687144",
            "__io_nav_state": "%7B%22current%22%3A%22%2F%22%2C%22currentDomain%22%3A%22thanhnien.vn%22%2C%22previousDomain%22%3A%22%22%7D",
            "__uidac": "21f04643af9139a4e97d258247e92d1f",
            "__admUTMtime": "1736749687",
            "AviviD_already_exist": "0",
            "AviviD_sw_version": "1.0.868.210701",
            "AviviD_session_id": "1736749687688",
            "_gid": "GA1.2.1689435480.1736749688",
            "_fbp": "fb.1.1736749689123.44475216864107514",
            "AviviD_landing_count": "1",
            "__RC": "194",
            "__R": "0",
            "AviviD_is_pb": "0",
            "AviviD_max_time_no_move": "0",
            "AviviD_max_time_no_scroll": "0",
            "AviviD_max_time_no_click": "0",
            "AviviD_max_time_pageview": "0",
            "AviviD_max_time_pageview_total": "0",
            "AviviD_max_scroll_depth_page_last": "2",
            "AviviD_time_pageview_total": "0",
            "AviviD_click_count_total": "0",
            "AviviD_scroll_depth_last": "2",
            "AviviD_scroll_depth_px_last": "100",
            "AviviD_max_scroll_depth": "2",
            "_uidcms": "4011366871422050388",
            "show_avivid_native_subscribe": "2",
            "_ga_N7ERK8YH08": "GS1.1.1736749687.1.0.1736749873.60.0.0",
            "_ga": "GA1.1.924288048.1736749684",
            "orig_aid": "hk0xvynn58qr7tfa.1736749873.des",
            "fosp_uid": "hk0xvynn58qr7tfa.1736749873.des",
            "AviviD_refresh_uuid_status": "2",
            "am_FPID": "f80877a8-38b4-45f9-bb4e-35cdd9fa96d9",
            "am_FPID_JS": "f80877a8-38b4-45f9-bb4e-35cdd9fa96d9",
            "cto_bundle": "L2OwAV9PT1JQUkJIcFlMNFJhcloxd0pnNGdWalU3ZmVGUERjQWU3VlpyYVd3THlhbUM0SVcxc21VTFdLT01BeXRZQ2xyejIxRnJ3S1BWVWxPdGlMNm9OeVlCbm1xZW9EenhnQVNuZUl5b1k5YW1IWWN6JTJGSXNuNmNvdCUyQmU1JTJGQ3J6MGYlMkZjSmlnNXZjbmtQcVdPQlRtdUZCOFFrQSUzRCUzRA",
            "cto_bidid": "skYtBV8xMWxIY21xZHU5RzhEc2NISWp0bm03T2JrVVBtY1JwSDRHJTJCQ2duakgzYTFZZ2swRU9qTCUyQjlrZDc0U0R0dHE2cll1eDc2UmtVRDJjREtiZDhOZkR0anclM0QlM0Q",
            "AviviD_pageviews": "2",
            "AviviD_max_pageviews": "2",
            "cto_dna_bundle": "5wYGcV9PT1JQUkJIcFlMNFJhcloxd0pnNGdSZjV4Y3BGUGg5dk5mZU01djluS0glMkJKUzQ4QmYzM2xMTm9rVk9LbEVQaHNzNVZ4c3M3MXZya25ZWkxZc1l3NDZRJTNEJTNE",
            "__uif": "__uid%3A4011366871422050388%7C__ui%3A-1%7C__create%3A1736749686",
            "__tb": "0",
            "__IP": "2335962906",
            "_ga_DDKGVNZ9BG": "GS1.1.1736749683.1.1.1736750080.60.0.0"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")

        links = response.xpath("//h3[@class='box-title-text']/a/@href").extract()
        print(links)
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
        url = f"https://thanhnien.vn/timelinesearch/{current_keyword}/{current_page}.htm"
        params = {
            "author": "0",
            "time": "0",
            "zone": "0",
            "type": "1",
            "sort": "0"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        content = "".join(response.xpath("//div[@class='detail-cmain']//p/text()").extract())
        items.content = content
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
