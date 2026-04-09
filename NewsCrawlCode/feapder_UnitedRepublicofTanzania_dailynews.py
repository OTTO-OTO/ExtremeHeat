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

    country = 'United Republic of Tanzania'
    table = 'United_Republic_of_Tanzania'
    #英语
    keywords = ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = 'https://dailynews.co.tz/wp-json/csco/v1/more-posts'
            data = {
                "action": "csco_ajax_load_more",
                "page": "1",
                "posts_per_page": "10",
                "query_data": json.dumps({
                    "first_post_count": 10,
                    "infinite_load": False,
                    "query_vars": {
                        "s": keyword,
                        "error": "",
                        "m": "",
                        "p": 0,
                        "post_parent": "",
                        "subpost": "",
                        "subpost_id": "",
                        "attachment": "",
                        "attachment_id": 0,
                        "name": "",
                        "pagename": "",
                        "page_id": 0,
                        "second": "",
                        "minute": "",
                        "hour": "",
                        "day": 0,
                        "monthnum": 0,
                        "year": 0,
                        "w": 0,
                        "category_name": "",
                        "tag": "",
                        "cat": "",
                        "tag_id": "",
                        "author": "",
                        "author_name": "",
                        "feed": "",
                        "tb": "",
                        "paged": 0,
                        "meta_key": "",
                        "meta_value": "",
                        "preview": "",
                        "sentence": "",
                        "title": "",
                        "fields": "",
                        "menu_order": "",
                        "embed": "",
                        "category__in": [],
                        "category__not_in": [],
                        "category__and": [],
                        "post__in": [],
                        "post__not_in": [],
                        "post_name__in": [],
                        "tag__in": [],
                        "tag__not_in": [],
                        "tag__and": [],
                        "tag_slug__in": [],
                        "tag_slug__and": [],
                        "post_parent__in": [],
                        "post_parent__not_in": [],
                        "author__in": [],
                        "author__not_in": [],
                        "search_columns": [],
                        "post_type": "post",
                        "ignore_sticky_posts": False,
                        "suppress_filters": False,
                        "cache_results": True,
                        "update_post_term_cache": True,
                        "update_menu_item_cache": False,
                        "lazy_load_term_meta": True,
                        "update_post_meta_cache": True,
                        "posts_per_page": 10,
                        "nopaging": False,
                        "comments_per_page": "50",
                        "no_found_rows": False,
                        "search_terms_count": 2,
                        "search_terms": ["heavy", "rain"],
                        "search_orderby_title": [
                            f"wp7m_posts.post_title LIKE '%heavy%'",
                            f"wp7m_posts.post_title LIKE '%rain%'"
                        ],
                        "order": "DESC"
                    },
                    "in_the_loop": False,
                    "is_single": False,
                    "is_page": False,
                    "is_archive": False,
                    "is_author": False,
                    "is_category": False,
                    "is_tag": False,
                    "is_tax": False,
                    "is_home": False,
                    "is_singular": False
                }),
                "attributes": "undefined",
                "options": '{"location":"archive","meta":"archive_post_meta","layout":"grid","columns":3,"image_orientation":"landscape-16-9","image_size":"csco-medium","summary_type":"summary","excerpt":false}',
                "_ajax_nonce": "7b2150841b"
            }
            yield feapder.Request(url, data=data, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://dailynews.co.tz",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "sec-ch-ua-mobile": "?0"
        }
        request.cookies = {
            "__wpdm_client": "def7b37c0a64abcd9b4ee36bc6d3a599",
            "_ga": "GA1.1.1632756801.1736736204",
            "__gads": "ID",
            "__gpi": "UID",
            "__eoi": "ID",
            "_ga_5B0TT1B3ZS": "GS1.1.1736736204.1.1.1736736589.59.0.0",
            "FCNEC": "%5B%5B%22AKsRol_7DII2C1i05nzHzjILVSM2lQH-6VG9HVXKQKSGJ8sHy75hT0h3B0i3puJ5Ccs-LRR9EU0UEEryNxwCJjSOh1dBK-CTIJB1wurl-GChHyLybbAfJ9UPSVW900y3HQGtQZVk1rDgn81oWXg05JZxw5d8AUxcJQ%3D%3D%22%5D%5D"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        data = response.json['data']['content']
        html = etree.HTML(data)
        links = html.xpath('//article//h2/a/@href')
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
        url = "https://turkmenistan.gov.tm/en/find"
        params = {
            "page": f"{current_page}",
            "key": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1//text()").extract_first()
        content = "".join(response.xpath("//div[@class='entry-content']//p/text()").extract())
        items.content = content
        items.author = ''
        items.pubtime = ''
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
