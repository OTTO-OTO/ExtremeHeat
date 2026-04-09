
import json
import re
import time
import uuid

import feapder
from feapder import Item
from feapder.db.mysqldb import MysqlDB

class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.101.200", port=3307, db="spider_keywords", user_name="czm", user_pass="root"
    )
    sql = """ select db_name from keywords where country='Canada' and language='英语'"""
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
    country = 'Canada'
    table = 'Canada'
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
    keywords = \
        db.find(f"select keywords_list from keywords where language = '英语' and country='{table}'", to_json=True)[
            0].get(
            "keywords_list")
    if isinstance(keywords, str):
        keywords = json.loads(keywords)
    print("待抓取的关键词列===========>", keywords)

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.cbc.ca/search_api/v1/search"
            params = {
                "q": f"{keyword}",
                "sortOrder": "relevance",
                "section": "news",
                "media": "all",
                "boost-cbc-keywords": "7",
                "boost-cbc-keywordscollections": "7",
                "boost-cbc-keywordslocation": "4",
                "boost-cbc-keywordsorganization": "3",
                "boost-cbc-keywordsperson": "5",
                "page": "1",
                "fields": "feed"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "baggage": "sentry-environment=production,sentry-release=ocelot%406d84abcce837e8458081c5d7b0d410f50e512dfb,sentry-public_key=fef33bc017414f09bcf117882ad6b34a,sentry-trace_id=f665a6034a53444fbf8326e0cbafa971,sentry-sample_rate=0.1,sentry-transaction=%2F%3Asection(search),sentry-sampled=false",
            "priority": "u=1, i",
            "referer": "https://www.cbc.ca/search?q=heavy%20rain&section=news&sortOrder=relevance&media=all",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "sentry-trace": "f665a6034a53444fbf8326e0cbafa971-a7feab211e07161a-0",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "NSC_mcwt-ttm-xfcgbsn.dcd.db": "ffffffff0983169745525d5f4f58455e445a4a423660",
            "AMCVS_951720B3535680CB0A490D45%40AdobeOrg": "1",
            "bm_mi": "86D33A22FAA6E7DD32554333728D4931~YAAQHtYsFzD5v2GUAQAAxTgochqLdnWCEWr9ald9V7V2fkjX3//5KrCxUtABEFfpLPn2Ky6gXQKxRfxNKRkHLQC7BklGmu4W8Y2wkVKaxdnTTujIQcJ2jXnNE5AzafxCs6uphXuWNPUmBigODqAHQQEZmyoT1LNnbVxTWm65cpMfHYzcyCZcKC73BVYpgnaU4mP1YpPfsV/ecujkrXH4UmpeO5LWMJV0D2dqGK3XllgBv5KMzKfHDBPw6cFkQCrBJU9fssGuVMEGiYHOap8iTU0XVLdJz2S5VHsj00QKCRUqsQiCIhTFeI2di9iAQSr9YqA+i7lGm0B6eP3KzI1CDLgyavHprdRdFVo7eIPpEUmST9ChkKKhufGSoKYzAWzZuPdEdCsM5Q==~1",
            "s_ecid": "MCMID%7C59297485759687874840610788131948133286",
            "cbc_visitor": "47ce5b3b-f5b9-47b6-90f2-41b87170d399",
            "ak_bmsc": "A35A7BD5E36CFD920D192569C9DAE763~000000000000000000000000000000~YAAQHtYsF836v2GUAQAAg0Qochp3qR00MjWaQ/XBvZh57BNemWWL8btykLkDjcH01b+JdL8kOBR1OtdeQscGUHAt72re4ODYInI/Kshwim+VGNOUb90k6/aC4wl9Lw5pcAOBtHQ8CplHBlOis4xENo6MX2OMMR7UbnkaS9zr//l5gMi9UBMJ0MpVSZDu9cUyr/2YMcdQGbzJ3OWkZS35X53uOjNNRtLEy46sXOCzRIcwfncLIJDUk9oyfLj/7Pmqy0clkSmg4VABybxy4jvReVYOigVnV6kFXpDAwlFjmdxNzl8LWdG7rUtKCe8Rwks7mAHBCq9iiDDRH2enz6Fzkmzdsz1BFCxti+g4c5lDsyT1RJ0cDCBjFztvlu/5LWYoJkQY4HGc1T+hhYcaR5NL5UHSZnTEMlKEVDrCFuX7Tm0/vNdrG78L3z1//LCWRCV6kjYHfQ+Z7BE5eubC0d9Jhmq0yGW/6MKK0BD33J2hXOQt8g+qwi1TqaVeulHRCHlFU92yoyrpFOf1gHfBQGqW4HtaX0QhjDQS8z12gg==",
            "cbc_ppid": "47ce5b3b-f5b9-47b6-90f2-41b87170d399",
            "_pctx": "%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAEzIE4AmHgZgDZuARgCsvDhwAMAdgAsADmm8QAXyA",
            "referrerPillar": "feed",
            "cbc-session": "1737082046",
            "_pcid": "%7B%22browserId%22%3A%22m605sseojtyg40mq%22%7D",
            "cX_P": "m605sseojtyg40mq",
            "AMCV_951720B3535680CB0A490D45%40AdobeOrg": "1585540135%7CMCIDTS%7C20106%7CMCMID%7C59297485759687874840610788131948133286%7CMCAAMLH-1737686840%7C7%7CMCAAMB-1737686840%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1737089242s%7CNONE%7CMCAID%7CNONE%7CMCSYNCSOP%7C411-20113%7CvVersion%7C4.4.0",
            "s_cc": "true",
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "panoramaId": "c2ba4404dd1662779c299379ee5816d53938f08b6896985ba839d86735aa250d",
            "panoramaIdType": "panoIndiv",
            "_vfa": "www%2Ecbc%2Eca.00000000-0000-4000-8000-082f79d5b829.69cb96d1-74e8-49af-88cc-cbf5b41169c5.1737082046.1737082046.1737082046.1",
            "_vfz": "www%2Ecbc%2Eca.00000000-0000-4000-8000-082f79d5b829.1737082046.1.medium=direct|source=|sharer_uuid=|terms=",
            "_fbp": "fb.1.1737082048031.546715961988693173",
            "_scor_uid": "7fd62bc28bf9437dbc782d134c171d8b",
            "_cb": "I4dciCxvhtoCuv-80",
            "_cb_svref": "external",
            "_t_tests": "eyI3bnFGdElrVGtjVk1RIjp7ImNob3NlblZhcmlhbnQiOiJCIiwic3BlY2lmaWNMb2NhdGlvbiI6WyJDOUx0bnIiXX0sImxpZnRfZXhwIjoibSJ9",
            "cX_G": "cx%3A15odhibm6nubv1w0onnm4o27q7%3A29ul3zmqlkqbu",
            "BCSessionID": "e8aa5f0b-1c75-4f92-bcba-1f0ecbf06d63",
            "last_visit_bc": "1737082069916",
            "cbc_app_version": "6d84abcce837e8458081c5d7b0d410f50e512dfb",
            "_awl": "2.1737082054.5-b0029ab53b43658cd2106bf4f568aaed-6763652d617369612d6561737431-0",
            "referringDepartment": "noreferrer",
            "_chartbeat2": ".1737082048180.1737082070911.1.BqeEOPLL43FzwZwDGA_Gew02c0.2",
            "_chartbeat5": "",
            "panoramaId_expiry": "1737686855903",
            "bm_sv": "BF36DC04F2615A109FD665EB5AB464E2~YAAQHtYsF7wFwGGUAQAAKq8ochqz2w+woSr9GTib0k0jWhln/w0ZnzgbPeJ9tkdka0xVuyn5xWERhnOIuZBkZkgbkBCuORF5IU56QeHEfZHOH7x64gFcL8kikeKxVstjmbzECrr7K/FacX+nkPyzX06NJvi/C6hmhDLrgbmgJqkcOAfCV+VCqXmUgo18y6/Cs93sx7v53f7QatJYWEmSW0X8pZmzorbc/sgvzkb+tNFcT3v7mt3zgFi9rFIkbfZ8BA==~1",
            "DATA_FEATURE_LINKS": "",
            "SC_LINKS": "",
            "_vfb": "www%2Ecbc%2Eca.00000000-0000-4000-8000-082f79d5b829.4.10.1737082046....",
            "cp-sess": "%7B%22traits%22%3A%5B%5D%2C%22sels%22%3A%7B%7D%2C%22rwds%22%3A%7B%7D%2C%22vn%22%3A1%2C%22tvts%22%3A1737082045%2C%22vts%22%3A1737082105%2C%22vals%22%3A%7B%22dt%2Fwp%22%3A%7B%22v%22%3A%22wd%22%2C%22ts%22%3A1737082105%7D%7D%7D"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json
        # print(json.loads(links))
        # # 输出匹配的值
        # for match in matches:
        #     print(match)

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
            if item.get("category0") == "player":
                continue
            items = Item()
            items.table_name = self.table
            items.article_url = "https:" + item.get("url")
            items.title = item.get("title")
            items.country = self.country
            items.pubtime = item.get("recs-publishtime")
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.cbc.ca/search_api/v1/search"
        params = {
            "q": f"{current_keyword}",
            "sortOrder": "relevance",
            "section": "news",
            "media": "all",
            "boost-cbc-keywords": "7",
            "boost-cbc-keywordscollections": "7",
            "boost-cbc-keywordslocation": "4",
            "boost-cbc-keywordsorganization": "3",
            "boost-cbc-keywordsperson": "5",
            "page": f"{current_page}",
            "fields": "feed"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        # items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath(
            "//div[@class='story']//p/text()").extract()).strip()
        items.author = ''
        # items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
