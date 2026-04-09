import json

import feapder
from NewsItems import SpiderDataItem
from lxml import etree

class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=10,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[5, 8],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
    )
    country = 'Belgium'
    table = 'Belgium_rtbf'
    keywords = [
    'extrême',
    'chaleur',
    'températures élevées',
    'fortes pluies',
    'sécheresse',
    "panne d'électricité due à la chaleur",
    'incendie',
    "pollution de l'air",
    'changement climatique',
    'réduction des rendements agricoles',
    'hypoxie',
    'coup de chaleur',
    'impact de la chaleur sur le trafic',
    'désastre écologique',
    "impact du changement climatique sur l'économie",
    'vague de chaleur marine',
    'pollution liée à la chaleur',
    'corail'
]
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.rtbf.be/_next/data/rtbf/recherche/article.json"
            params = {
                "q": f"{keyword}",
                "page": "1",
                "type": "article"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page,keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=1, i",
            "referer": "https://www.rtbf.be/recherche/article?q=extr%C3%AAme&page=2",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "x-nextjs-data": "1"
        }
        request.cookies = {
            "didomi_token": "eyJ1c2VyX2lkIjoiMTkzZDdjMGQtMjU1Ni02MWM1LTk3MDQtYjk0MjEwOTA1ZGZlIiwiY3JlYXRlZCI6IjIwMjQtMTItMThUMDM6MTI6MzguNDg1WiIsInVwZGF0ZWQiOiIyMDI0LTEyLTE4VDAzOjEyOjQwLjM1NFoiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwiYW1hem9uIiwiYzpsaW5rZWRpbiIsImM6Z29vZ2xlYW5hLTRUWG5KaWdSIl19LCJwdXJwb3NlcyI6eyJlbmFibGVkIjpbImdlb2xvY2F0aW9uX2RhdGEiLCJkZXZpY2VfY2hhcmFjdGVyaXN0aWNzIl19LCJ2ZXJzaW9uIjoyLCJhYyI6IkFGbUFDQUZrLkFBQUEifQ==",
            "euconsent-v2": "CQJ1KUAQJ1KUAAHABBENBSFsAP_gAAAgAAIgJyQFgAKgAXABAACoAFoAMgAaABFACYAFKALYAuABtAEAAIQAaUA_QD-AIGAUgA_YCmgF5gTkApqACApoAUJABgACCqg6ADAAEFVCEAGAAIKqEoAMAAQVUKQAYAAgqoAA.f_wAAAQAAAAA",
            "_cb": "CiZZ-YCBOfNyCpD8yT",
            "_fbp": "fb.1.1734491563397.4311479833994450",
            "gig_bootstrap_3_kWKuPgcdAybqnqxq_MvHVk0-6PN8Zk8pIIkJM_yXOu-qLPDDsGOtIDFfpGivtbeO": "login_ver4",
            "_ga": "GA1.1.2069382631.1734491567",
            "_gcl_au": "1.1.1739316663.1734491567",
            "__gfp_64b": "Esy1jSeWuaeEXvLPkuaz93DYx7mT3nWI_QsvgCrtLlr.P7|1734491554|2|||8:1:80",
            "_cb_svref": "external",
            "_chartbeat2": ".1734491561698.1735628161236.10000000000001.BIGznjDWYE6zC3LfOGBLa4ehBV5qXx.4",
            "_ga_XQ6LWJBT9C": "GS1.1.1735628008.2.1.1735628167.0.0.0",
            "_chartbeat5": "841|4615|%2Frecherche%2Farticle%3Fq%3Dextr%25C3%25AAme%26page%3D2|https%3A%2F%2Fwww.rtbf.be%2Frecherche%2Farticle%3Fq%3Dextr%25C3%25AAme%26page%3D3|DaVSM5B510QiCJHvilCuKxiCBhe6Pr||c|ZwJSEBVNAYbB6C_iJDTlWDwqxHGe|rtbf.be|"
        }
        return request


    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json['pageProps'].get("results")
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            items = SpiderDataItem()
            items.article_url = item.get("share").get("canonical")
            items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)


        current_page = request.page + 1
        url = "https://www.rtbf.be/_next/data/rtbf/recherche/article.json"
        params = {
            "q": f"{current_keyword}",
            "page": f"{current_page}",
            "type": "article"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@id='content']//div//p/text()").extract())
        items.author = ''
        items.pubtime = json.loads(response.xpath("//script[@type='application/ld+json'][2]/text()").extract_first()).get("datePublished")
        print(items)
        # if items.content:
        #     yield items

if __name__ == "__main__":
    AirSpiderDemo().start()
