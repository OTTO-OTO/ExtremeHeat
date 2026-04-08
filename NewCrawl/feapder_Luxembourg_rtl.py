import feapder
from feapder import Item
from lxml import etree
import re


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[5, 8],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB="spider_data",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    country = 'Luxembourg'
    table = 'Luxembourg_rtl'
    keywords =['Extreme',
               # 'Heat','High Temperature','Heavy Rain','Drought','Power Outage from Heat','Fire','Air Pollution','Climate Change','Crop Yield Reduction','Oxygen Deficiency','High Temperature Affecting Traffic','Ecological Disaster','Climate Change Affecting Economy','Marine Heatwave','High Temperature Pollution','Coral'
               ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://today.rtl.lu/api/search/curator"
            params = {
                "q": f"{keyword}",
                "start": "0",
                "limit": "20"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=1, i",
            "referer": "https://today.rtl.lu/search?q=heavy%20rain&p=3",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "didomi_token": "eyJ1c2VyX2lkIjoiMTk0Mzk0NWItMzIyNS02MWJiLTkzYmEtODk2ZGU5ZDAyZjI5IiwiY3JlYXRlZCI6IjIwMjUtMDEtMDZUMDE6NDA6NTYuNzM4WiIsInVwZGF0ZWQiOiIyMDI1LTAxLTA2VDAxOjQxOjI1LjI1MVoiLCJ2ZXJzaW9uIjoyLCJwdXJwb3NlcyI6eyJlbmFibGVkIjpbImRldmljZV9jaGFyYWN0ZXJpc3RpY3MiLCJnZW9sb2NhdGlvbl9kYXRhIiwiZ2VvX21hcmtldGluZ19zdHVkaWVzIiwiZ2VvX2FkcyJdfSwidmVuZG9ycyI6eyJlbmFibGVkIjpbImdvb2dsZSIsInR3aXR0ZXIiLCJjOmlwZGlnaWFsLUNwa2pVeG1jIiwiYzpnb29nbGVhbmEtWThpVWVyNloiLCJjOnlvdXR1YmUiLCJjOmdpZ3lhLWNvdW50ZXIiLCJjOmdpZ3lhIiwiYzppbmZvZ3JhbS1EV01OZDl4NyIsImM6bGlua2VkaW4iLCJjOnJlZGRpdCIsImM6ZWRpdHVzLWMyajhGM1BuIiwiYzphc2lsZWx1LTROVzhqS0pkIiwiYzpkaXZlcnMtOWdlUHhiZGkiLCJjOmdyYWNlbm90ZS1wUkU5RVdMYyIsImM6cXVhbGlmaW8tZjd4WXdOMnciLCJjOm9uZXNpZ25hbC13ZFIyRlJNeCIsImM6dGlrdG9rLUtaQVVRTFo5IiwiYzpucGF3LXpuTjgyV0VDIiwiYzppbnN0YWdyYW0iXX0sInZlbmRvcnNfbGkiOnsiZW5hYmxlZCI6WyJnb29nbGUiXX0sImFjIjoiQ25XQUVBRmtGT29BLkNuV0FFQUZrRk9vQSJ9",
            "euconsent-v2": "CQKzyIAQKzyIAAHABBENBWFsAP_gAEPgABagKvNX_G__bWlr8X73aftkeY1P9_h77sQxBhfJE-4FzLvW_JwXx2ExNA36tqIKmRIAu3TBIQNlGJDURVCgaogVryDMaEyUoTNKJ6BkiFMRM2dYCFxvm4tjeQCY5vr991dx2B-t7dr83dzyy4xHn3a5_2S0WJCdA5-tDfv9bROb-9IOd_x8v4v4_F_pE2_eT1l_tWvp7D9-cts__XW99_fff_9Pn_-uB_-_X_vf_H3wVdAJMNCogDLAkJCDQMIIEAKgrCAigQBAAAkDRAQAmDAp2BgAusJEAIAUAAwQAgABBkACAAACABCIAIACgQAAQCBQABgAQDAQAEDAACACwEAgABAdAxTAggECwASMyKhTAhCASCAlsqEEgCBBXCEIs8AiAREwUAAAAABSAAICwWBxJICVCQQBcQbQAAEACAQQAFCCTkwABAGbLUHgwbRlaYBg-YJENMAyAIgjISDAAAAA.f_wACHwAAAAA",
            "__gfp_64b": "cVwR.sIGXlp7xEz4minKx3ea1QePnqFLIz8482q7M4f.g7|1736127675|2|||8:1:80",
            "_gid": "GA1.2.799341241.1736127686",
            "permutive-id": "102c127d-2946-4615-9bfa-c65e1520f5a2",
            "gig_bootstrap_3_GrIH4wyVdEGBWtbHuPmCh669d0ReOliTAHH4tePPwELds6CIg0fnuMzeJ5Dr68FY": "gigya_ver4",
            "_ga_VCBQ3RV8YE": "GS1.1.1736127687.1.1.1736127822.60.0.0",
            "gig_bootstrap_3_7ve4Cpl33kwsFCtxLWk4axI2yVBGlFM1hQZA82FDMrG0h4lMx5eUNb1iaqxBHmCy": "gigya_ver4",
            "_ga": "GA1.1.76558698.1736127686",
            "_ga_8NSCFCGHZ8": "GS1.1.1736127832.1.1.1736128060.60.0.0"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json['items']
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            # print(item)
            items = Item()
            items.article_url = item.get("link")
            items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 20
        url = "https://today.rtl.lu/api/search/curator"
        params = {
            "q": f"{current_keyword}",
            "start": f"{current_page}",
            "limit": "20"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='article__body article-body']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//div[@class='article-heading__metainfo']/text()[last()]").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
