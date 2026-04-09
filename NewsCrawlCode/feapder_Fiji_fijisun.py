import feapder
from feapder import Item
from curl_cffi import requests
from lxml import etree


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[6, 8],
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

    country = 'Fiji'
    table = 'Fiji_fijisun'
    keywords = ['Extreme',
                # 'Heat', 'High Temperature', 'Heavy Rain', 'Drought', 'Power Outage from Heat', 'Fire',
                # 'Air Pollution', 'Climate Change', 'Crop Yield Reduction', 'Oxygen Deficiency',
                # 'High Temperature Affecting Traffic', 'Ecological Disaster', 'Climate Change Affecting Economy',
                # 'Marine Heatwave', 'High Temperature Pollution', 'Coral'
                ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://fijisun.com.fj/page/1/"
            params = {
                "s": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://fijisun.com.fj/?s=heat",
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
            "legatus_post_views_count_457016": "1",
            "__utma": "191051709.1123837961.1735873834.1735873834.1735873834.1",
            "__utmc": "191051709",
            "__utmz": "191051709.1735873834.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)",
            "_ga": "GA1.1.655230956.1735873834",
            "mo_cDrbXAxfpE": "true",
            "legatus_post_views_count_620081": "1",
            "__utmt": "1",
            "__gads": "ID=9cc5e58691ec4e80:T=1735873831:RT=1735874927:S=ALNI_Mb7O1y3cOeAnCxWFA_JHC07tvGOwg",
            "__gpi": "UID=00000fd1df8bcaff:T=1735873831:RT=1735874927:S=ALNI_Manvjpttgze4v_KgSFvA0Q-xzzVKw",
            "__eoi": "ID=993f47c9ad237447:T=1735873831:RT=1735874927:S=AA-AfjbVU1PUQgau9jYSoZI8ZNLV",
            "legatus_post_views_count_615293": "1",
            "__utmb": "191051709.6.9.1735874996049",
            "FCNEC": "%5B%5B%22AKsRol_U4YZPTIKEnVL4jNVVRvLyVgt_QvjW0GH0_QSLwxwZrnSripbCC3iHvEAUsjTGiOshsfCXMk826Y6OlM7M2QdKDSO-Qb5JFGe1UDmfpvB_NeBy7bfybBn0k2iPrSJQzD5AGzAAHva-jw9_P2w2HkCxrhvcbw%3D%3D%22%5D%5D",
            "_ga_ZEN5TSC87N": "GS1.1.1735873834.1.1.1735875003.51.0.0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//div[@class='article-header']/h2/a/@href").extract()
        print(links)
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            print(item)
            items = Item()
            items.article_url = item
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://fijisun.com.fj/page/{current_page}/"
        params = {
            "s": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        # print(response.text)
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='shortcode-content']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//div[@class='date']/text()").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
