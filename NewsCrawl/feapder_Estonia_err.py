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

    country = 'Estonia'
    table = 'Estonia_err'
    keywords = [
    'Ekstremne', 'Külm', 'Kõrge temperatuur', 'Tugev viis', 'Kuivus',
    'Külma tõttu stroomi katkestus', 'Tuline', 'Õhukorraldus', 'Kliimamuutused',
    'Põlletoodangi vähenemine', 'Hääl', 'Kõrge temperatuur mõjutab liiklus',
    'Ekoloogiline õud', 'Kliimamuutused mõjutavad majandust', 'Meresooline laine',
    'Kõrgemate temperatuuride saastamine', 'Korall'
]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.err.ee/api/search/getContents/"
            params = {
                "options": f"{{\"total\":0,\"page\":1,\"limit\":50,\"offset\":50,\"phrase\":\"{keyword}\",\"publicStart\":\"\",\"publicEnd\":\"\",\"timeFromSchedule\":false,\"types\":[],\"category\":109}}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page,keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=1, i",
            "referer": "https://www.err.ee/search?phrase=K%C3%B5rge%20temperatuur&page=1",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "__cf_bm": "_VgHq35KG3omYRrK_ST7Ky8Ze5DM_aOG9tuY0wB4nZc-1735871227-1.0.1.1-AoY90ffBrVoa_0SEXituhS.I.2v9GQErv3rPBKegx9ErylKPcMp3w6kIxfOuJT6VDnRJrDex94ZjfC.tBI4wjO9.ZnB6JfujC8FL.8VzBxc",
            "_cfuvid": "N3iNFcRynZmVfgtPGKTHPjlCjqKC7Rc9cVX8CSy96eE-1735871227823-0.0.1.1-604800000",
            "_ga": "GA1.1.1160202462.1735871240",
            "_cb": "B5NVi2DB4iMhCUoLep",
            "_cb_svref": "external",
            "statUniqueId": "5f742137d7c93b913ed97f188d2a4642",
            "atlId": "a25bt1ph0a8lropbq9a5vnecml",
            "cookiesession1": "678A3E1429D79D1D441B199CD379082C",
            "__gfp_64b": "gCTKq3.HPbuvgmW5JpBxln8StSJFVGar9CDKXU7JpLv.l7|1735871233|2|||8:1:80",
            "cf_clearance": "SVLC6mXMshALFRmXgVmbm.GiQCtmv4mPeexOkMnHcwM-1735871233-1.2.1.1-Bgn8ZGlA.Ch6J7ClhIFSnXw9.5Efg7_hgpsEKXp6oaEaDfOZFVW6CCNHkAy8WFFbjQ0GOhr7SREqnhU2LkegtcrL1FuCBHWghRPrywFSGeom4n.H9H367KriqGOJvUJez.BIKdf.9RvVQjJTCtBAj1FraLcB3v4BIH5Wpvf8vlZu51V0oY2VW.nByJcBWuIe8FlP7eV07AairxeDw6PdTMznXLATk8Wp2Uz.rEpUfzut386rFBHp_e46EGSIQdAU_enm2UPvjlgoZiXOE8bs8qgFGN.3jPTsM.22PvBaeNARCuayIMTLMj6Afi6HW1ereOJ86CgJkjnsZgFqIN0GPILRbjt4TK..xX0.OA7YyTmY9if3rMX_T9SiDdFu5DozIOe4BqVRNgc3TBVAue3VJQ",
            "_chartbeat2": ".1735871240088.1735871623360.1.KVobtCwILe7DuY8f1BnY5M7B3YuI2.3",
            "_ga_D5MN2FNBRC": "GS1.1.1735871239.1.1.1735871681.0.0.0",
            "_ga_8C7W76BC1S": "GS1.1.1735871239.1.1.1735871681.0.0.0",
            "_chartbeat5": "249|7854|%2Fsearch%3Fphrase%3DEkstremne|https%3A%2F%2Fwww.err.ee%2Fsearch%3Fphrase%3DK%25C3%25B5rge%2520temperatuur%26page%3D1|DGWqY0DuMMoPfellBaaodeDMs96f||c|DGWqY0DuMMoPfellBaaodeDMs96f|err.ee|"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json["contents"]
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            # print(item)
            items = Item()
            items.article_url = item.get("url")
            items.title = item.get("heading")
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.err.ee/api/search/getContents/"
        params = {
            "options": f"{{\"total\":0,\"page\":{current_page},\"limit\":50,\"offset\":50,\"phrase\":\"{current_keyword}\",\"publicStart\":\"\",\"publicEnd\":\"\",\"timeFromSchedule\":false,\"types\":[],\"category\":109}}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,keyword=current_keyword)

    def parse_detail(self, request, response):
        # print(response.text)
        items = request.items
        items.table_name = self.table
        # items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='body']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        # if items.content:
        #     yield items

if __name__ == "__main__":
    AirSpiderDemo().start()
