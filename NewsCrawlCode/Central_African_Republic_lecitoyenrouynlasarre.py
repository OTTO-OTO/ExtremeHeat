import feapder
import requests
from NewsItems import SpiderDataItem

from feapder.db.mysqldb import MysqlDB

# 没有翻页
class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.101.200", port=3307, db="spider_keywords", user_name="czm", user_pass="root"
    )
    sql = """ select db_name from keywords where country='Central_African_Republic' and language='法语'"""
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
    country = 'Central African Republic'
    table = 'Central_African_Republic'
    #法语
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
    keywords = db.find(f"select keywords_list from keywords where language = '法语' and country='{table}'", to_json=True)[0].get(
        "keywords_list")
    print("待抓取的关键词列===========>", keywords)
    def start_requests(self):
        for keyword in self.keywords:
            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                "cache-control": "max-age=0",
                "content-type": "application/x-www-form-urlencoded",
                "origin": "https://www.lecitoyenrouynlasarre.com",
                "priority": "u=0, i",
                "referer": "https://www.lecitoyenrouynlasarre.com/recherche",
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
            cookies = {
                "PHPSESSID": "55ubh6jnbref8bjvpsdl62o5i5",
                "lang": "fr",
                "_gid": "GA1.2.367795074.1735543994",
                "_ga": "GA1.2.1944069356.1734569612",
                "_ga_FPT1K7YVZL": "GS1.1.1735543992.4.1.1735544331.0.0.0"
            }
            url = "https://www.lecitoyenrouynlasarre.com/validate_form.php"
            data = {
                "g-recaptcha-response": "03AFcWeA5cMC7W-Yxuc96e-SesAzom07uCT8RCVJxzA5gh55a0PcrWZB2pUukJw1TYlH9ioRFCs5ynbMAa4gC1WRlgomGXAV9mF5KzyPmANL8Dgo_cAt0ILXUDykdG4eV_61kiBRAKCcAoIE9GoU83GJ6JG0X-7K9vZHF5T-w4pjdtCgPKvHsnSicODQW8ESEAGmFAGi4mMHrLYRc0BM55L2Q43sUs7POscm1MmN8IFAegSCKvFw5ABR0GKOjJotj0TGRpRgRHy9JF2544XA71wL02zibvBBbqDmzhyRfRZM-CqWbIqvzGf0iX9_tFRG3eY4OUN_a696rpdmUNJi1XQQ1dYqZ-2UxNjyjPgIO9-5Qi3kcE04G_3pdJBGW8xWK7ykRkcFubwBgCvjQN4g0Q-rF8KN_bBXPRt9rOEfwrJhLwqaDbuCjeVf0q5OnIe5zVnArD8XLUoF2U8MEC2BV_xdU4N6_CYGNhaNDF3Nn2hxgj8tRAEkqu32gkGSIg0CeDpagNJkPkz2r6AlZh3RhBkSyv8KrlStnlahic9wrfAwzdjMpyourM4Ejz2exSnUWtMZ0Xz2eyBfe-vELwhOopRyBZBBs1xaUe_ANrgO8QQg76jVRXp8Zue29o90Wo_yfrplE2BtzwfHijXqznhKO_2qziXKYaGQb0o7J_pFET37GZ9D3ew3qwZHUj26CFSzvG12Bl2N_1MeBeuw3hrxuwsHpcGED34bLO33Q-nYGVqFnAs5oGF21tJZlBFgVLXfG5dbj0rxShJeYAJyv95huxlFC8kcCLG78xkRoqOok_iKNvBOUFnLgBJV0vksUx6f59PdT0vtBXaN9mTFH9foP8Yh94cPZF9h3L255QOoLYdauw3lkvcnvVtNDVxGzUzu4vGR8gcs-hqu1z",
                "form": "search",
                "search_query": f"{keyword}",
                "journalist": "Veuillez sélectionner un journaliste",
                "start_date": "01-01-2000",
                "end_date": "30-12-2024",
                "categories%5B%5D": "203|248|293|3|446|492|538|584|347|395|634|682|730|820|869"
            }
            response = requests.post(url, headers=headers, cookies=cookies, data=data)
            if response.status_code == 200:
                url = "https://www.lecitoyenrouynlasarre.com/recherche"
                yield feapder.Request(url,callback=self.parse_url,keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "referer": "https://www.lecitoyenrouynlasarre.com/recherche",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0"
        }
        request.cookies = {
            "PHPSESSID": "5em7g98smprspc00gorvqpup66",
            "lang": "fr",
            "_gid": "GA1.2.2012417729.1739242345",
            "_ga": "GA1.2.152264558.1739242345",
            "_ga_FPT1K7YVZL": "GS1.1.1739242344.1.1.1739242565.0.0.0"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        url_lists = response.xpath("//a[@class='img_txt_02 table']/@href").extract()
        print(url_lists)
        for url in url_lists:
            items = SpiderDataItem()
            items.article_url = url
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=url,callback=self.parse_detail,items=items)

    def parse_detail(self,request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@id='content']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items

if __name__ == "__main__":
    AirSpiderDemo().start()
