# 付费
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
    sql = """ select db_name from keywords where country='Colombia' and language='西班牙语'"""
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
    country = 'Colombia'
    table = 'Colombia'
    #西班牙语
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
    keywords = db.find(f"select keywords_list from keywords where language = '西班牙语' and country='{table}'", to_json=True)[0].get(
        "keywords_list")
    print("待抓取的关键词列===========>", keywords)
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.noticiascaracol.com/busqueda"
            params = {
                "q": f"{keyword}",
                "p": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.noticiascaracol.com/busqueda?q=calor",
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
            "_ga": "GA1.1.1292596646.1737082818",
            "_fbp": "fb.1.1737082819095.249836546304590286",
            "_scor_uid": "2f7394b084c64e72829ffc1237cb4dda",
            "_clck": "ybbkck%7C2%7Cfsn%7C0%7C1843",
            "nvg35578": "15b8ec8ec2991f3d7ca7782b5410|0_18",
            "didomi_token": "eyJ1c2VyX2lkIjoiMTk0NzIzNDUtOWEwYy02ZTZmLWFjZmMtODFjYTEyYmZiYzYyIiwiY3JlYXRlZCI6IjIwMjUtMDEtMTdUMDM6MDA6MjEuMDI0WiIsInVwZGF0ZWQiOiIyMDI1LTAxLTE3VDAzOjAwOjIyLjE5OFoiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwiYW1hem9uIiwidHdpdHRlciIsInNhbGVzZm9yY2UiLCJjOm1haWRlbm1hci00OURRTmZ6RyJdfSwicHVycG9zZXMiOnsiZW5hYmxlZCI6WyJnZW9sb2NhdGlvbl9kYXRhIiwiZGV2aWNlX2NoYXJhY3RlcmlzdGljcyJdfSwidmVuZG9yc19saSI6eyJlbmFibGVkIjpbImdvb2dsZSJdfSwidmVyc2lvbiI6MiwiYWMiOiJEQVdBRUJpSUdBb0EuREFXQUVCaUlHQW9BIn0=",
            "euconsent-v2": "CQLYCcAQLYCcAAHABBENBYFsAP_gAEPgAARwJnNX_G__bWlr8X73aftkeY1P99h77sQxBhfJE-4FzLvW_JwXx2ExNA36tqIKmRIAu3TBIQNlGJDURVCgaogVryDMaEyUoTNKJ6BkiFMRI2dYCFxvm4tjeQCY5vr991dx2B-t7dr83dzyy4hHn3a5_2S0WJCdA5-tDev9bROb-9IOd_x8v4v4_F_pE2_eT1l_tWvp7D9-cts_9XWwTOAJMNCogDLAkJCDQMIIEAKgrCAigQBAAAkDRAQAmDAp2BgAusJEAIAUAAwQAgABBkACAAACABCIAIACgQAAQCBQABgAQCAQAEDAACACwEAgABAdAxTAggECwASMyKhTAhCASCAlsqEEgCBBXCEIs8AiAREwUAAAAABSAAICwWBxJICVCQQBcQbQAAEACAQQAFCCTkwABAGbLUHgAAAA.f_wACHwAAAAA",
            "compass_uid": "4946b699-f178-4837-a5a6-024078131d86",
            "_ga_KJ5KBCFN6Z": "GS1.1.1737082818.1.1.1737083011.0.0.0",
            "_ga_68NGN9HSGY": "GS1.1.1737082818.1.1.1737083011.60.0.0",
            "_ga_8K2PEHFRTZ": "GS1.1.1737082818.1.1.1737083011.60.0.0",
            "_ga_2D8PW27P8J": "GS1.1.1737082818.1.1.1737083012.0.0.0",
            "__gsas": "ID=cefedf60c98af0cb:T=1737082997:RT=1737082997:S=ALNI_Ma8lRP8gOgWWsfcSzFWb_guDg4uvQ",
            "_clsk": "zm7d0t%7C1737083013017%7C2%7C0%7Cf.clarity.ms%2Fcollect",
            "___nrbic": "%7B%22previousVisit%22%3A1737083012%2C%22currentVisitStarted%22%3A1737083012%2C%22sessionId%22%3A%22b6b343df-2f28-4bf1-863b-aafbebd89787%22%2C%22sessionVars%22%3A%5B%5D%2C%22visitedInThisSession%22%3Atrue%2C%22pagesViewed%22%3A1%2C%22landingPage%22%3A%22https%3A//www.noticiascaracol.com/busqueda%3Fq%3Dcalor%23nt%3Dnavsearch%22%2C%22referrer%22%3A%22https%3A//www.noticiascaracol.com/%22%2C%22lpti%22%3Anull%7D",
            "___nrbi": "%7B%22firstVisit%22%3A1737083012%2C%22userId%22%3A%224946b699-f178-4837-a5a6-024078131d86%22%2C%22userVars%22%3A%5B%5B%22mrfExperiment_RecommenderAB%22%2C%222%22%5D%5D%2C%22futurePreviousVisit%22%3A1737083012%2C%22timesVisited%22%3A1%7D"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//ul[@class='SearchResultsModule-results']//h2[@class='PromoB-title']/div/a/@href").extract()
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
            items = Item()
            items.table_name = self.table
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            # items.pubtime = item.get("recs-publishtime")
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.noticiascaracol.com/busqueda"
        params = {
            "q": f"{current_keyword}",
            "p": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        # items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(
            response.xpath("//div[@class='ArticlePage-articleContainer']//p/text() | //div[@class='RichTextArticleBody-body RichTextBody']//p/text()").extract()).strip()
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
