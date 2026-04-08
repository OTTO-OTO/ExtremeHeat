import json

import feapder
from NewsItems import SpiderDataItem


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[5, 8],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.42.183",
        MYSQL_PORT=3306,
        MYSQL_DB="spider_data",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=False,  # 禁用 item 去重，使用数据库唯一索引确保不重复

    )

    country = 'Australia'
    table = 'Australia'
    #英语
    keywords =  ["Extreme", "Heat", "High", "Temperature", "Heavy", "Rain", "Drought", "Power", "Outage", "Fire", "Air", "Pollution", "Climate", "Change", "Crop", "Yield", "Reduction", "Oxygen", "Deficiency", "Affecting", "Traffic", "Ecological", "Disaster", "Economy", "Marine", "Heatwave", "Coral"]

    def __init__(self):
        super().__init__()
        self.previous_links = {}  # 每个关键词独立的链接跟踪

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://y63q32nvdl-dsn.algolia.net/1/indexes/*/queries"
            params = {
                "x-algolia-agent": "Algolia for JavaScript (4.18.0); Browser (lite); instantsearch.js (4.54.0); react (18.2.0); react-instantsearch (6.38.1); react-instantsearch-hooks (6.38.1); JS Helper (3.13.3)",
                "x-algolia-api-key": "bcdf11ba901b780dc3c0a3ca677fbefc",
                "x-algolia-application-id": "Y63Q32NVDL"
            }
            data = {
                "requests": [
                    {
                        "indexName": "ABC_production_all",
                        "params": f"clickAnalytics=true&facets=%5B%22site.title%22%5D&getRankingInfo=true&hitsPerPage=10&maxValuesPerFacet=20&page=1&query={keyword}&ruleContexts=%5B%22global_search%22%5D&tagFilters=&userToken=anonymous-2dc2af07-86ce-496e-a9d6-c4623497e0b9"
                    }
                ]
            }
            data = json.dumps(data, separators=(',', ':'))
            yield feapder.Request(url, params=params, data=data, callback=self.parse_url, page=page, keyword=keyword,
                                  method='POST')

    def download_midware(self, request):
        request.headers = {
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Origin": "https://discover.abc.net.au",
            "Referer": "https://discover.abc.net.au/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "cross-site",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "content-type": "application/x-www-form-urlencoded",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        data = response.json["results"][0].get("hits")
        if not data:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理

        # 提取链接列表
        current_links = [item.get("canonicalURL") for item in data if item.get("canonicalURL")]
        
        # 检查当前关键词的上一页链接
        if current_keyword in self.previous_links and current_links == self.previous_links[current_keyword]:
            print(f"关键词 {current_keyword} 的第 {request.page} 页链接与上一页相同，退出当前关键字的循环")
            return None
        
        self.previous_links[current_keyword] = current_links  # 更新当前关键词的链接列表
        
        for item in data:
            items = SpiderDataItem()
            items.article_url = item.get("canonicalURL")
            print(items.article_url)
            items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://y63q32nvdl-dsn.algolia.net/1/indexes/*/queries"
        params = {
            "x-algolia-agent": "Algolia for JavaScript (4.18.0); Browser (lite); instantsearch.js (4.54.0); react (18.2.0); react-instantsearch (6.38.1); react-instantsearch-hooks (6.38.1); JS Helper (3.13.3)",
            "x-algolia-api-key": "bcdf11ba901b780dc3c0a3ca677fbefc",
            "x-algolia-application-id": "Y63Q32NVDL"
        }
        data = {
            "requests": [
                {
                    "indexName": "ABC_production_all",
                    "params": f"clickAnalytics=true&facets=%5B%22site.title%22%5D&getRankingInfo=true&hitsPerPage=10&maxValuesPerFacet=20&page={current_page}&query={current_keyword}&ruleContexts=%5B%22global_search%22%5D&tagFilters=&userToken=anonymous-2dc2af07-86ce-496e-a9d6-c4623497e0b9"
                }
            ]
        }
        data = json.dumps(data, separators=(',', ':'))
        yield feapder.Request(url, params=params, data=data, callback=self.parse_url, page=current_page,
                              keyword=current_keyword)

    def parse_detail(self, request, response):
        # print(response.text)
        items = request.items
        items.table_name = self.table
        # items.title = response.xpath("//title/text()").extract_first()
        
        # 尝试多个XPath选择器来提取正文内容
        content_selectors = [
            "//p[@class='paragraph_paragraph__iYReA']//text()",
            "//div[@class='article__content']//p//text()",
            "//div[@class='content']//p//text()",
            "//div[@class='article-content']//p//text()",
            "//main//p//text()",
            "//article//p//text()"
        ]
        
        content = ""
        for selector in content_selectors:
            content_parts = response.xpath(selector).extract()
            if content_parts:
                content = "".join(content_parts)
                break
        
        items.content = content
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
