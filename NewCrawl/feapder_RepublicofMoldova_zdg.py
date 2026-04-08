import json

import feapder
from feapder import Item
from lxml import etree
import re


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

    country = 'Republic of Moldova'
    table = 'Republic_of_Moldova'
    #罗马尼亚语
    keywords = ["Ciclon tropical", "Depresiune tropicala", "Furtuna tropicala", "Tifon", "Uragan", "Ciclon", "Furtuna", "Ploi torentiale", "Inundatie", "Val de apa", "Daune coastale", "Alunecare", "Dezastre geologice", "Dezastre maritime", "Vanturi puternice", "Dezastre provocate de tifon", "Alunecare de teren", "Alunecare de pamant"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://www.zdg.md/wp-admin/admin-ajax.php"
            data = {
                "s": f"{keyword}",
                "cache_results": "true",
                "update_post_term_cache": "true",
                "lazy_load_term_meta": "true",
                "update_post_meta_cache": "true",
                "post_type": "any",
                "posts_per_page": "15",
                "comments_per_page": "50",
                "search_terms_count": "2",
                "order": "DESC",
                "operator": "OR",
                "orderby": "date",
                "template": "templates/components/list-item",
                "offset": "0",
                "action": "zdg_get_media_posts"
            }
            yield feapder.Request(url, data=data, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://www.zdg.md",
            "priority": "u=1, i",
            "referer": "https://www.zdg.md/?s=temperatur%C4%83+extrem%C4%83",
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
            "_ga": "GA1.1.2130346731.1736390520",
            "store.test111": "",
            "am-uid-f": "5353a906-c484-46f5-8348-7504a6549e89",
            "__gfp_64b": "y64aQs_xgxzvbAYRISWm9Qe2SzpLDUxAxzZQ6OhHkRv..7|1736390560|2|||8:1:80",
            "_sharedid": "87edaf59-0ef0-40ef-95df-bc43e4397823",
            "_sharedid_cst": "zix7LPQsHA%3D%3D",
            "panoramaId_expiry": "1736995365478",
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "panoramaId": "3d6e8b78d1ec91ae6e29e218acf4185ca02c60046cf65798b2f03d4a5c35863c",
            "am-uid": "5353a906c48446f583487504a6549e89",
            "_ga_VF704GSXJL": "GS1.1.1736390519.1.1.1736390954.31.0.0"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//h4[@class='list-item__title']/a/@href").extract()
        current_page = request.page

        current_links = links
        if self.previous_links is not None and current_links == self.previous_links:
            print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理

        self.previous_links = current_links  # 更新上一页的链接列表
        if request.page >= 75:
            print(f"关键词 {current_keyword} 的第 {request.page} 页已经抓取完毕，退出当前关键字的循环")
            return None
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            print(item)
            items = Item()
            items.table_name = self.table
            items.article_url = item
            # items.title = item
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 15
        url = "https://www.zdg.md/wp-admin/admin-ajax.php"
        data = {
            "s": f"{current_keyword}",
            "cache_results": "true",
            "update_post_term_cache": "true",
            "lazy_load_term_meta": "true",
            "update_post_meta_cache": "true",
            "post_type": "any",
            "posts_per_page": "15",
            "comments_per_page": "50",
            "search_terms_count": "2",
            "order": "DESC",
            "operator": "OR",
            "orderby": "date",
            "template": "templates/components/list-item",
            "offset": f"{current_page}",
            "action": "zdg_get_media_posts"
        }
        yield feapder.Request(url, data=data, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(
            response.xpath("//div[@class='single-post__content wpb_text_column']//p//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
