import feapder
from feapder import Item
from lxml import etree
import re


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=3,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[5, 8],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
    )
    country = 'Cyprus'
    table = 'Cyprus_cyprus_main'
    keywords = ['Extreme', 'Heat', 'High Temperature', 'Heavy Rain', 'Drought', 'Power Outage from Heat', 'Fire',
                'Air Pollution', 'Climate Change', 'Crop Yield Reduction', 'Oxygen Deficiency',
                'High Temperature Affecting Traffic', 'Ecological Disaster', 'Climate Change Affecting Economy',
                'Marine Heatwave', 'High Temperature Pollution', 'Coral']

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://cyprus-mail.com/search/page/1"
            params = {
                "q": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "referer": "https://www.jutarnji.hr/",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "script",
            "sec-fetch-mode": "no-cors",
            "sec-fetch-site": "cross-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "__Secure-3PSID": "g.a000rggyq3rN6l5a4RzrqfKR8L6w_4oSKSt2-NpZPRMnn-bzQbIYlvrEO6wyUoA2X7nR4dKaLQACgYKAc8SARcSFQHGX2Mi1LRifzVo7z1Yh0GO1dLi3BoVAUF8yKq57cjKXq68AYkR7eUXDFJW0076",
            "__Secure-3PAPISID": "IMadT4_VMCUqIjYe/AiQnZJDT7r7FsfPRc",
            "__Secure-3PSIDTS": "sidts-CjIB7wV3sRTW2BDd0msBJgPWSbfxzfyUhPt-3z4GoZ9EeJz5O-NDtLtmILNWAZobCVFE4hAA",
            "NID": "520=KeQk0Xb3ADfmLni5No_6PBM-Y5v3vdKKHFZl5VlNeIkdom1A3-hXSmYE0nqr5dZLXFFDxfdsF4qbMyGb5eI68evJ_qy0cOHFJEfpOewUMSJS_Jj8aQuTNKn-V-zKQlhN1cNQOUUDQqB-pWFxnOU2cSn70IfK5ycQFj4Nygzrt2CgX8N2LMQIhRl3nSvzJ_1q6Q44aOO5ROIdbFb2_FsHOPiQCK33zsvv7QY7ouePluclgQqQly44SkCydNBMlrdZCX95KuqEKqma9ND5g6CHWEurqqFBkJMFv596odBa8M5PY8h4Uqjd3_UynlmHMCQQGzJX99nQQq6S0tjghMRAzfPB_5TTqXn0I39hxWwTFywJzq5ChJD_DW_AwWjAhFCTkHV4SJ4O7sOpKha2en03FVtouSgo1BtjbUmyqnLrN66Ac-V3jEt6la15kzQzlV4Ltbt2JILVgBhMpBo9alPe7nrkGji9ZPB8CAs4nKVo7zaArXS09dDojxDyT5Fn-objIj9rDJj-G-t2PKh82HeHUDdDnHf4LJ5zxhEL9cxtiPv_7ZOqL4mcpg0xDDbJI6U5kpvtc13YNjtoi0Cr9pY2Z0CQ8qx4ajMEPpTFysVCxfzbj1k7Rh4euK-fhjTZ8i1ggfQ7riygh0LKvrCvHNX6nSX9n-oT6kQa8v9F_4P4QPy-bWsWEOfgaJCmkh6d9Vym7QCta2cvm9pHzVLK_tU-WDOCQAL7nZpNg8EJhiBjjzB_-DxnACBWgKjUfVK83c_Mjh9uLxEH0y1wfDkmXmOLM0NHOikqU7bBfc8oQR1WyQ",
            "__Secure-3PSIDCC": "AKEyXzXX3ffaEQu_WX011cRS8NKRCzARvH96NYcUKu7P_O8LrkhI5Vd3NGx1biohocVV6TDmKb4"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//a[@class='_lnkTitle_cekga_5']/@href").extract()
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
        url = f"https://cyprus-mail.com/search/page/{current_page}"
        params = {
            "q": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@id='articleBody']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
