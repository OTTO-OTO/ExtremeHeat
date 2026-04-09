import json

import feapder
from feapder import Item
from lxml import etree
import re


from feapder.db.mysqldb import MysqlDB

class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.101.200", port=3307, db="spider_keywords", user_name="czm", user_pass="root"
    )
    sql = """ select db_name from keywords where country='Romania' and language = '英语'"""
    mysql_db = db.find(sql, to_json=True)[0].get("db_name")
    print("待写入的数据库是:", mysql_db)
    # 判断数据库是否存在
    db.execute(f"CREATE DATABASE IF NOT EXISTS `{mysql_db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[8, 10],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB=f"{mysql_db}",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Romania'
    table = 'Romania'
    #英语
    create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS `{mysql_db}`.`{table}`  (
              `id` int NOT NULL AUTO_INCREMENT,
              `title` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '标题',
              `author` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '作者',
              `keyword` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '关键词',
              `content` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '内容',
              `article_url` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '文章网址',
              `pubtime` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '发布时间',
              `country` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '国家',
              `news_source_country` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '新闻来源国家',
              `place` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '地名',
              `Longitude_latitude` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '经纬度',
              `english` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '英文',
              `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '写入时间',
              PRIMARY KEY (`id`) USING BTREE,
              UNIQUE INDEX `title_uni`(`keyword` ASC, `article_url` ASC) USING BTREE
            ) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

                """
    db.execute(create_table_sql)
    print(f"{table}创建成功<=================")
    keywords = db.find(f"select keywords_list from keywords where language = '英语' and country='{table}'", to_json=True)[0].get(
        "keywords_list")
    print("待抓取的关键词列===========>", keywords)
    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://adevarul.ro/content.json"
            params = {
                "query_json": f'{{"limit":33,"search_text":"{keyword}","prevent_page_duplicates":false,"published":[1],"published_at":["1990-01-01",null],"sort":[{{"column":"published_at","order":"desc"}}],"domain":["adevarul.ro"],"page":0,"offset":0}}'
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "content-type": "application/json",
            "priority": "u=1, i",
            "referer": "https://adevarul.ro/search?q=temperaturi+extreme",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "OptanonAlertBoxClosed": "2025-01-09T03:41:45.970Z",
            "eupubconsent-v2": "CQK8koAQK8koAAcABBROBXF8AP_gAAAAAChQKdNV_G__bXlr8Xr3aftkeY1f99h77sQxBhfJk-4FyDvW_JwX32EyNA26tqYKmRIAuzRBIQFlGJDURVCgSogVrzDMYECUgTNKJ6BkgFMRY2dYCFxvmYtjeQCY5vp9d1d52R-t7dr83dzyy4Bnv3Y5P2QlUICdA58tDfn9bRKb-5IOd-x8v4vw9N_pE2_eTVl_tWvp7B8uctu__XV99_cAAEAAAAAAAAA_XwAAAGBIKIACAAFwAUABUADgAHgAQQAvADUAHgARAAmABVADeAHoAPwAhIBDAESAI4ASwAmgBWgDAAGHAMoAywBsgDngHcAd8A9gD4gH2AfsA_wEAAIpARcBGACNQEiASWAn4CgwFQAVcAuYBegDFAGiANoAbgA4kCPQJFATsAocBR4CkQFNgLYAXIAu8BeYDDYGRgZIAycBmYDOYGrgayA28BuoDggHJgOXAeOA_4CCYEGAIQwQtBC6CHoIfwR9BH8CRQEkIJMAkyBLMCW4EvgJgATMAmcBNwIAmAAcACQAI4AnACDgEcAL6AlYBMoCbQFIQKfAqEBYQCxAFuALyAX-AxABiwDIQGjANTAbQA24BugDyoHyAfKA_cCAgEDAIIgQrAh4BFMCVIExQJmgTUHARgAEQAOAA8AC4AJAAfgBHADQAI4AcgBAICDgIQAREAjgBUADpAJFASsAmIBMoCbQFJgKhAVKArsBYgC1AFuALoAX-AwQBiADFgGQgMmAaMA1MBrwDaAG3AN0AceA5YBz4DygHxAPlAfbA_YD9wICAQMAgeBBECDYEKwIeARTAjABG8CQgEjYJIgkkBKkCXAEuwJggTDAmaBNQCbAE2x0GEABcAFAAVAA4ACCAFwAagA8ACIAEwAKsAXABdADEAG8APQAfoBDAESAJYATQAnABRgCtAGAAMMAZQA0QBsgDngHcAd4A9oB9gH6AP-AigCMQEdASWAn4CgwFRAVcAsQBc4C8gL0AYoA2gBuADiAHUAPsAi-BHoEiAJkATsAoeBR4FIQKaApsBVgCxQFsALdAXAAuQBdoC7wF5gL6AYaAx6BkYGSAMnAZUAywBmYDOQGmgNVgauBrADbwG6gOLAcmA5cB44D6wH3AQBAgwBC0CHQEPYI-gj-BIoCSAEmQJZgS6Al8BMACZgEzgJuATeAnCQASgAIAAeAGgAcgBHACxAF9ATaApMBUoCxAF5AMEAZ4A0YBqYDbgG6AOWAc-A8oB8QD9gICAQPAg2BCsCGYEUwIwARpAjfBJEEkgJhgTNAmwBNshAqAAWABQAFwANQAqgBcADEAG8APQAjgBgADngHcAd4A_wCKAEpAKDAVEBVwC5gGKANoAdQBHoCmgFWALFAWiAuABcgDIwGTgM5AaqA8cCDAELQIdAQ9AkUBJACXQEziUDAABAACwAKAAcAB4AEQAJgAVQAuABigEMARIAjgBRgCtAGAANkAd4A_ICogKuAXMAxQB1AETAIvgR6BIgCjwFNALFAWwAvOBkYGSAMnAZyA1gBt4EAQIHgQYAhCBD0CRQEkAJdAS-AmYBM4CbgE4SQB0AC4ARwB3AEAAIOARwAqACRQErAJiATaApMBbgC_wGLAMsAZ4A3IBugDlgHlAP3AgIBBECGYEYAJJATNAm2UgoAALgAoACoAHAAQQAyADQAHgARAAmABSACqAGIAP0AhgCJAFGAK0AYAAygBogDZAHOAO-AfgB-gEWAIxAR0BJQCgwFRAVcAuYBeQDFAG0ANwAdQA9oB9gETAIvgR6BIgCdgFDgKQgU0BTYCrAFigLYAXAAuQBdoC8wF9AMNgZGBkgDJwGWAM5gawBrIDbwG6gOCAcmA8UB44D_gIJgQYAhCBC0CGcEOQQ6gj6CP4EigJIQSYBJkCWYEugJfATAAmYBM4CbxQBwABcAEgALgAjgCOAE4AOQAdwA-wCAAEHALEAXUA14B2wD_gJFATEAm0BT8CpAKlAV2AtwBdAC8gGCAMWAZMAzwBowDUwGvAN0AcsA8oB8QD5QH2wP2A_cCAgEDAIHgQbAhWBDwCKYEYAJGwSRBJICVIEuwJmgTUAmwBNsCcJaAYADUAYAA7gC9AH2AU0AqwBmYDxwIegTMAm4WAGgDLAI4Aj0BMQCbQGjANTAboA5YCAgEYAJmgTY.f_wAAAAAAAAA",
            "_ga": "GA1.1.805884256.1736394107",
            "_pctx": "%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAEzIEYOBmAVg4AMAJgEA2IQE5RAgBwCA7KInyQAXyA",
            "_pcid": "%7B%22browserId%22%3A%22m5os7vzkbyvazs47%22%7D",
            "_cb": "BUpWt8DG8icxa-kJ3",
            "_clck": "281rfw%7C2%7Cfsf%7C0%7C1835",
            "_pubcid": "0bc623b3-430c-4c73-9ee9-47aee3af728a",
            "_pubcid_cst": "zix7LPQsHA%3D%3D",
            "cX_G": "cx%3A3jps5gqxkxwvv2b7m57wkel4rn%3A5d1kpx7ob5yj",
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "panoramaId_expiry": "1736998902136",
            "panoramaId": "3d6e8b78d1ec91ae6e29e218acf4185ca02c60046cf65798b2f03d4a5c35863c",
            "panoramaIdType": "panoDevice",
            "33acrossIdTp": "0JeFwXMAMblWsjVg7HtqaEQnvv08KhLRJxUOKBWDIf4%3D",
            "_au_1d": "AU1D-0100-001736394132-ID4S0YIF-9NRI",
            "_cb_svref": "external",
            "__gads": "ID=d46cd0d92f223f68:T=1736394303:RT=1736400229:S=ALNI_MaVUPevNa760cyEbj9oo_r_K0TC-Q",
            "__gpi": "UID=00000fda84b90bbb:T=1736394303:RT=1736400229:S=ALNI_MYXrLOQ9m697hadwxxD0F2jYuivlA",
            "__eoi": "ID=2ab473178c9b404e:T=1736394303:RT=1736400229:S=AA-AfjYOIe3s-XDqoyZTHUrRFo7H",
            "_ga_FVWZ0RM4DH": "GS1.1.1736399598.2.0.1736400251.60.0.0",
            "_chartbeat5": "",
            "OptanonConsent": "isGpcEnabled=0&datestamp=Thu+Jan+09+2025+13%3A24%3A44+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202408.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=d46274da-3c9a-41c0-83e8-3d597dccaa33&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0010%3A1%2CBG2106%3A1%2CC0001%3A1%2CC0008%3A1%2CC0003%3A1%2CC0002%3A1%2CC0004%3A1%2CC0009%3A1%2CC0011%3A1%2CC0007%3A1%2CC0005%3A1%2CV2STACK1%3A1&intType=1&geolocation=%3B&AwaitingReconsent=false",
            "_chartbeat2": ".1736394107657.1736400284465.1.J6kczBFoUZ1CI1pIHD73edCYHMyR.3",
            "_ga_RM6F9P86YL": "GS1.1.1736399598.2.1.1736400284.16.0.0",
            "cX_P": "m5os7vzkbyvazs47",
            "_tfpvi": "M2IyMjQ1MWEtMWJkYy00MWNjLTg5ODYtNGY0YjgzNWFlMDEwIzQw",
            "FCNEC": "%5B%5B%22AKsRol91QFIItGlTFQq-Gml94UeRu7eutOvZ0Q5BNAji4gC1LNvUGBW_396SzqNSU4XW8o9_cMU6Yyu60GApVt7mWtqn3RF48Bz2Bgz6fB8aPiX5VVD9eluGmp_V7DbeD2uhWgopZUMbYeRLcc0IL4Kjl-qJAGuufg%3D%3D%22%5D%5D",
            "_clsk": "1vc0tiv%7C1736400285444%7C3%7C1%7Cf.clarity.ms%2Fcollect"
        }
        return request

    def parse_url(self, request, response):
        print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        # print(response.json)
        links = response.json['items']
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
            # print(item)
            items = Item()
            items.table_name = self.table
            items.article_url = item.get("url")
            items.title = item.get("title")
            items.country = self.country
            items.pubtime = item.get("published_at")
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 33
        url = "https://adevarul.ro/content.json"
        params = {
            "query_json": f'{{"limit":33,"search_text":"{current_keyword}","prevent_page_duplicates":false,"published":[1],"published_at":["1990-01-01",null],"sort":[{{"column":"published_at","order":"desc"}}],"domain":["adevarul.ro"],"page":0,"offset":{current_page}}}'
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        # items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(
            response.xpath("//article//main//p/text()").extract())
        items.author = ''
        # items.pubtime = ''
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
