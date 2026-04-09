import json

import feapder
from NewsItems import SpiderDataItem



from feapder.db.mysqldb import MysqlDB

class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.101.200", port=3307, db="spider_keywords", user_name="czm", user_pass="root"
    )
    sql = """ select db_name from keywords where country='Peru' and language = '西班牙语'"""
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
    country = 'Peru'
    table = 'Peru'
    # 西班牙语
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
            url = f"https://elcomercio.pe/buscar/{keyword}/todas/descendiente/1/"
            yield feapder.Request(url, method="GET", callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://elcomercio.pe/buscar/Sequ%C3%ADa+/todas/descendiente/2/",
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
            "akaas_AS_elcomercio_elcomercio_prod": "2147483647~rv=73~id=48fd3b4b5bee9fc4b93605bbc5f49387",
            "_gcl_au": "1.1.1294815336.1735176966",
            "_ga": "GA1.1.469118485.1735176966",
            "compass_uid": "e883cdb4-3d2b-4436-97a7-5040ea9a7ff4",
            "_tt_enable_cookie": "1",
            "_ttp": "rIvhTotMR7q8_qNeD2FxklUyvT2.tt.1",
            "_fbp": "fb.1.1735176967793.195487815724327308",
            "_pctx": "%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAEzIEYOBWDgBi4B2AGwAOQR2F8ATAGYALB0EgAvkA",
            "gecdigarc": "d0945818af85008954baea51c87af7af",
            "___nrbi": "%7B%22firstVisit%22%3A1735176966%2C%22userId%22%3A%22e883cdb4-3d2b-4436-97a7-5040ea9a7ff4%22%2C%22userVars%22%3A%5B%5B%22mrfExperiment_experimentoInline%22%2C%221%22%5D%5D%2C%22futurePreviousVisit%22%3A1735176966%2C%22timesVisited%22%3A1%2C%22userType%22%3A0%7D",
            "_pc_user_status": "no",
            "_pcid": "%7B%22browserId%22%3A%22m54nkfhd9yj19f3p%22%7D",
            "__pid": ".elcomercio.pe",
            "__pat": "-18000000",
            "cX_G": "cx%3A35dkjg3nuz6zj113cg8q3bcklk%3A1ic421kdgywnr",
            "__pnahc": "0",
            "___nrbic": "%7B%22isNewUser%22%3Atrue%2C%22previousVisit%22%3A1735176966%2C%22currentVisitStarted%22%3A1735176966%2C%22sessionId%22%3A%22e8969951-7676-4ffe-8904-8775742fe106%22%2C%22sessionVars%22%3A%5B%5D%2C%22visitedInThisSession%22%3Atrue%2C%22pagesViewed%22%3A3%2C%22landingPage%22%3A%22https%3A//elcomercio.pe/%22%2C%22referrer%22%3A%22%22%2C%22lpti%22%3Anull%7D",
            "_ga_NNH3LH5HP2": "GS1.1.1735176966.1.1.1735177021.5.0.0",
            "__pvi": "eyJpZCI6InYtbTU0bmtmaGtmaTJxZWQ5eiIsImRvbWFpbiI6Ii5lbGNvbWVyY2lvLnBlIiwidGltZSI6MTczNTE3NzAyMTgzNX0%3D",
            "__adblocker": "false",
            "__tbc": "%7Bkpex%7D0OiEzVkzHoEdOC2f8_MzjREJ0ALKNHljRYPxo6WA3Y7NRqL3TFYDLwoQsb5USNuc",
            "xbc": "%7Bkpex%7D0hXvSY5NSS9yPhRF_UksnUu4zyLRNost3Ch3rlgVeT8",
            "_pcus": "eyJ1c2VyU2VnbWVudHMiOnsiQ09NUE9TRVIxWCI6eyJzZWdtZW50cyI6WyJMVHM6NTU0ODY5OTJiZTI4YTY3MzdkZTZkZjE5MzFjY2JhNWJjNDM4MDU2YTpub19zY29yZSJdfX19",
            "cX_P": "m54nkfhd9yj19f3p",
            "RT": "\"z=1&dm=elcomercio.pe&si=f47cde62-7254-4a33-a6f2-cb14c65817b4&ss=m54nl02x&sl=2&tt=bcz&rl=1&nu=2l18dv6f&cl=1s5k&ld=1s5q&r=3l5cqobf&ul=1s5q\""
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//h2/a/@href").extract()
        current_page = request.page

        current_links = links
        if self.previous_links is not None and current_links == self.previous_links:
            print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理

        self.previous_links = current_links  # 更新上一页的链接列表
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理

        for url in links:
            print(url)
            items = SpiderDataItem()
            items.article_url = url
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://elcomercio.pe/buscar/{current_keyword}/todas/descendiente/{current_page}/"
        yield feapder.Request(url, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//section//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
