import feapder
from feapder import Item
from lxml import etree
from feapder.db.mysqldb import MysqlDB

class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.101.200", port=3307, db="spider_keywords", user_name="czm", user_pass="root"
    )
    sql = """ select db_name from keywords where country='Barbados' and language='英语'"""
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
    country = 'Barbados'
    table = 'Barbados'
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
            page = 1
            url = "https://nationnews.com/page/1/"
            params = {
                "s": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page,keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html, application/xhtml+xml",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "content-type": "application/json",
            "priority": "u=1, i",
            "referer": "https://armenpress.am/en/articles?query=heat&page=2",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "x-inertia": "true",
            "x-inertia-partial-component": "Articles/Search",
            "x-inertia-partial-data": "data,params",
            "x-inertia-version": "273aeeab46571b13b24a61246c77d9b8",
            "x-requested-with": "XMLHttpRequest",
            "x-xsrf-token": "eyJpdiI6InpObDRkYU5teWJBK0Y3ellpYzhXd0E9PSIsInZhbHVlIjoiSDVPYlZ3OWM2UzRTU25UbE9LZU5qdVN2c2VvTFk4TFpxR1h3VzVaeXBFajNuWkU0TS96anVrM3dSWGNINzRzMnJpNmlLdHJ0MXAxang0dW8zaFcrYTUxa1h0WUlSRGw2NU1Ydkc2b3RJUWYxdXl6SGpReExsK2s0dWoyUG1renYiLCJtYWMiOiJiZjg5NDg4MzNhOTQ0NmRiZmE3NThjYjIxYTkwOWY2ZDljYjI1ZjI0Y2I3Njk5YjBiZDU5NzYwZDc1MmNmNmU3IiwidGFnIjoiIn0="
        }
        request.cookies = {
            "_ga": "GA1.1.929206472.1734407052",
            "_ym_uid": "1734407054493860277",
            "_ym_d": "1734407054",
            "_ga_1F4J87EYB0": "GS1.1.1734413932.2.1.1734416460.55.0.0",
            "XSRF-TOKEN": "eyJpdiI6InpObDRkYU5teWJBK0Y3ellpYzhXd0E9PSIsInZhbHVlIjoiSDVPYlZ3OWM2UzRTU25UbE9LZU5qdVN2c2VvTFk4TFpxR1h3VzVaeXBFajNuWkU0TS96anVrM3dSWGNINzRzMnJpNmlLdHJ0MXAxang0dW8zaFcrYTUxa1h0WUlSRGw2NU1Ydkc2b3RJUWYxdXl6SGpReExsK2s0dWoyUG1renYiLCJtYWMiOiJiZjg5NDg4MzNhOTQ0NmRiZmE3NThjYjIxYTkwOWY2ZDljYjI1ZjI0Y2I3Njk5YjBiZDU5NzYwZDc1MmNmNmU3IiwidGFnIjoiIn0%3D",
            "armenpress_armenian_news_agency_session": "eyJpdiI6IlIraHdWakZVcnZoa0N4dUNadE8yT3c9PSIsInZhbHVlIjoiLzRjOUFQbUR5NmY4RzIxWldHamRJZkZ6VXZ3aUFBKzZyR2c2T2RKMFlxdnNzdTRwSmlZYjhJbU94eXE5UUhmNklVc0Z4aGVWUmVWNFVqbkU2WHplUEkrQUNiczRXY0E5ckNwb0JNRnRjcHB3cEZmVUFvc2pmUEFaRHVIVUFGd0kiLCJtYWMiOiI2MTIyNTE1ZjcxMTQ5MTg5NDY1YzQyYWU5MWQ5YWMyY2M5OWIyZTU3NDkxMzhiYTAyYjA0NThiY2QwNWVjZDQ2IiwidGFnIjoiIn0%3D",
            "cfzs_google-analytics_v4": "%7B%22GRzp_pageviewCounter%22%3A%7B%22v%22%3A%226%22%7D%7D",
            "cfz_google-analytics_v4": "%7B%22GRzp_engagementDuration%22%3A%7B%22v%22%3A%220%22%2C%22e%22%3A1767149194856%7D%2C%22GRzp_engagementStart%22%3A%7B%22v%22%3A%221735613194856%22%2C%22e%22%3A1767149194856%7D%2C%22GRzp_counter%22%3A%7B%22v%22%3A%227%22%2C%22e%22%3A1767149194856%7D%2C%22GRzp_ga4sid%22%3A%7B%22v%22%3A%22233530957%22%2C%22e%22%3A1735614994856%7D%2C%22GRzp_session_counter%22%3A%7B%22v%22%3A%221%22%2C%22e%22%3A1767149194856%7D%2C%22GRzp_ga4%22%3A%7B%22v%22%3A%229c91c472-108e-44f2-9e81-21d4bedfe5fe%22%2C%22e%22%3A1767149194856%7D%2C%22GRzp_let%22%3A%7B%22v%22%3A%221735613194856%22%2C%22e%22%3A1767149194856%7D%7D"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//h3/a/@href").extract()[:6]
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            items = Item()
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)


        current_page = request.page + 1
        url = f"https://nationnews.com/page/{current_page}/"
        params = {
            "s": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//meta[@property='og:title']/@content").extract_first()
        items.content = "".join(response.xpath("//div[@class='tdb-block-inner td-fix-index']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items

if __name__ == "__main__":
    AirSpiderDemo().start()
