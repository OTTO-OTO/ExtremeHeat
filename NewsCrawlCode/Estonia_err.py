import feapder
from feapder import Item
from curl_cffi import requests
from lxml import etree
from feapder.db.mysqldb import MysqlDB

class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.101.200", port=3307, db="spider_keywords", user_name="czm", user_pass="root"
    )
    sql = """ select db_name from keywords where country='Estonia' and language = '爱沙尼亚语'"""
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
    country = 'Estonia'
    table = 'Estonia'
    #爱沙尼亚语
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
    keywords = db.find(f"select keywords_list from keywords where language = '爱沙尼亚语' and country='{table}'", to_json=True)[0].get(
        "keywords_list")
    print("待抓取的关键词列===========>", keywords)
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
        current_page = request.page

        current_links = links
        if self.previous_links is not None and current_links == self.previous_links:
            print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理
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
        if items.content:
            yield items

if __name__ == "__main__":
    AirSpiderDemo().start()
