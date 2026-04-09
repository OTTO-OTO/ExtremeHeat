import json

import feapder
from feapder import Item
from lxml import etree
import re


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[8, 10],
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
    country = 'Iraq'
    table = 'Iraq_alsumaria'
    keywords = [
    'شديد', 'حرارة', 'حرارة مرتفعة', 'مطر شديد', 'جفاف', 'انقطاع الكهرباء بسبب الحرارة', 'نار', ' загрязنание الهواء', 'التغير المناخي', 'تخفيض إنتاج المحاصيل', 'نقص الأكسجين', 'الحرارة مرتفعة تؤثر على المرور', ' thảمار بيئية', 'التغير المناخي يؤثر على الاقتصاد', 'موجة حرارة بحرية', ' загرязنание بسبب الحرارة المرتفعة', 'قرنيط'
]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://cse.google.com/cse/element/v1"
            params = {
                "rsz": "filtered_cse",
                "num": "10",
                "hl": "ar",
                "source": "gcsc",
                "start": f"0",
                "cselibv": "5c8d58cbdc1332a7",
                "cx": "010621872549312797348:kgbocjfadqm",
                "q": f"{keyword}",
                "safe": "off",
                "cse_tok": "AB-tC_4O4cziPB6GlsK3Y3TXpFba:1740360644117",
                "sort": "",
                "exp": "cc,apo",
                "callback": "google.search.cse.api14485",
                "rurl": f"https://www.alsumaria.tv/Search/{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "referer": "https://www.alsumaria.tv/",
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
            "__Secure-3PSIDTS": "sidts-CjIB7wV3sd8ufp4nver-MhiOP96KUtLmmcfiEtaoB-gQHYqUgSuK6hALYUPNjJnY8CrIcxAA",
            "NID": "520=cVXuowYi2H-ji-DyWXP93m0PfXmI6xX-gvxozKxKNV9OMnGRC0XtyHTbGrHsa05gDWJukvhE5BDmpG0z5xIy2-frYXvy-p3T6a2ncq6YBRQrAxj-pSH7MRDO3FLpshS0Y3xRyc_b6afDnT405OjW8DzuyzwOc1XfoVP94vlHH2ipmHp9Wt6NpFgoGtfAyyK_lg2z8bY16jlvqwjwjcUCFyDvDW86MYMPJjQf7FZ21SmQs2o0xkNG5Qjict1jzCDF8GGIq44AjPDEW3RiCK_DwRJyu3avCWdIDBIpZlTGr-ure9K_m67_eKNmQwOQmBEJZs-_3BZTKfMmyr-pAEalGhlb7_1QDmu4z5WvAK-gcULWS19iLA6-AwKwD0UaMlFB4UI0sLv00YQP9iLJGm1MbmP5m2aOC0eRShU57cG9HQQdwozYi4SQS5AQzUa6oUmz6AskaqeB9gQpPs12YgR_sRmNYO0lA5s2al2pwkcrwG4mIE6Y2K0DVqFP0ioT8YENemvOhd17DtvvEliQgVTEJv9DoHRFjUj9w3sfAB8OmE8pfdX-QuomZoOfjuaVvI1wjn_YA2rmydoXL2oJnqwqvnoZUYgR5ZtY-NwnSbl_3zNkpb3ddWLOnDRqyNUmNXLiOTlFyY9jj1I4xMNjcah2yersdBCAHAqYUmZSs1cfKMLMRz_3ET47ftaGNV_Ivv32HQ3JGQPubwQudKMCnwajPVeE9QMkdKg43YqaMN-MvlDToMKogOk4U2z-v7-EezyIFbtcXYSYBqEWPHoxYYxQ8aBikxr4p7IgmdW0oPwReg",
            "__Secure-3PSIDCC": "AKEyXzX8_M8PtJ91EAH-Wc2zt1IV04u8MR1gGghzwk-9OaljEeoIQ-mjK9tZaXMYqpvRVtD2CHY"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        data = response.text.split("api14485(")[-1].split(");")[0]
        links = json.loads(data)['results']
        # pattern = r'"url": "(.*?)"'
        # links = re.findall(pattern, data)
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links[:10]:
            # print(item)
            items = Item()
            items.article_url = item.get("unescapedUrl")
            items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 10
        url = "https://cse.google.com/cse/element/v1"
        params = {
            "rsz": "filtered_cse",
            "num": "10",
            "hl": "ar",
            "source": "gcsc",
            "start": f"{current_page}",
            "cselibv": "5c8d58cbdc1332a7",
            "cx": "010621872549312797348:kgbocjfadqm",
            "q": f"{current_keyword}",
            "safe": "off",
            "cse_tok": "AB-tC_4O4cziPB6GlsK3Y3TXpFba:1740360644117",
            "sort": "",
            "exp": "cc,apo",
            "callback": "google.search.cse.api14485",
            "rurl": f"https://www.alsumaria.tv/Search/{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        # items.title = response.xpath("//h1//text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='LongDesc']//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@itemprop='datePublished']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
