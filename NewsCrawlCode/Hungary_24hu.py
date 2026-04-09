# -*- coding: utf-8 -*-
"""

集群运行

"""
import re

import feapder
from feapder import Item
import json
from lxml import etree

from feapder.db.mysqldb import MysqlDB

class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.101.200", port=3307, db="spider_keywords", user_name="czm", user_pass="root"
    )
    sql = """ select db_name from keywords where country='Hungary' and language = '匈牙利语'"""
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

    country = 'Hungary'
    table = 'Hungary'
    #匈牙利语
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
    keywords = db.find(f"select keywords_list from keywords where language = '匈牙利语' and country='{table}'", to_json=True)[0].get(
        "keywords_list")
    print("待抓取的关键词列===========>", keywords)
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://24.hu/page/1/"
            params = {
                "s": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, method='GET',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://24.hu/?s=magas+h%C5%91m%C3%A9rs%C3%A9klet",
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
            "_ga": "GA1.1.1678634195.1739260498",
            "_fbp": "fb.1.1739260498189.748195026112527749",
            "_gcl_au": "1.1.418752843.1739260498",
            "SID": "sfgk7spnjcj39s88lv89ec7gp0",
            "sportWidgetVisible": "true",
            "campaigns": "{}",
            "browser_id": "93a4ee64-bf31-4b2a-af7a-de7373f84c9d",
            "remp_session_id": "5238e92c-0583-402c-b5eb-b62858c1ccac",
            "compass_uid": "6a847d96-f79d-4973-98f9-2aab6a366f00",
            "euconsent-v2": "CQMqb4AQMqb4AAKA1AHUBcFsAP_gAEPgAA6gKztX_G__bWlr8X73aftkeY1P99h77sQxBhbJE-4FzLvW_JwXx2E5NAz6tqIKmRIAu3TBIQNlHJDURVCgaogVrSDMaEyUoTNKJ6BkiFMRI2dYCFxvm4tjeQCY5vr991dx2B-t7dr83dzyy4hHn3a5_2S1WJCdA4-tDfv9bROb-9IOd_x8v4v4_F7pE2_eT1l_tWvp7D9-cts_9XW99_fbff9Pn_-uB_-_X_vf_H37uCsgBJhoVEAZZEhIQaBhBAgBUFYQEUCAIAAEgaICAEwYFOQMAF1hIgBACgAGCAEAAIMgAQAACQAIRABAAUCAACAQKAAMACAYCABgYAAwAWAgEAAIDoGKYEEAgWACRmRQKYEIACQQEtlQgkAQIK4QhFngEQCImCgAABAAKQABAWCwOJJASsSCALiCaAAAgAQCCAAoRSdmAIIAzZaq8GTaMrTAsHzBc9pgGQBEEZOSbEAA.d9gACCAAAAAA",
            "addtl_consent": "1~43.3.9.6.9.13.6.4.15.9.5.2.11.8.1.3.2.10.33.4.15.17.2.9.20.7.20.5.20.7.2.2.1.4.40.4.14.9.13.8.9.6.6.9.41.5.3.1.27.1.17.10.9.1.8.6.2.8.3.4.146.65.1.17.1.18.25.35.5.18.9.7.41.2.4.18.24.4.9.6.5.2.14.25.3.2.2.8.28.8.6.3.10.4.20.2.17.10.11.1.3.22.16.2.6.8.6.11.6.5.33.11.19.28.12.1.5.2.17.9.6.40.17.4.9.15.8.7.3.12.7.2.4.1.7.12.13.22.13.2.6.8.10.1.4.15.2.4.9.4.5.4.7.13.5.15.17.4.14.10.15.2.5.6.2.2.1.2.14.7.4.8.2.9.10.18.12.13.2.18.1.1.3.1.1.9.7.2.16.5.19.8.4.8.5.4.8.4.4.2.14.2.13.4.2.6.9.6.3.2.2.3.7.3.6.10.11.9.19.8.3.3.1.2.3.9.19.26.3.10.13.4.3.4.6.3.3.3.4.1.1.6.11.4.1.11.6.1.10.13.3.2.2.4.3.2.2.7.15.7.14.4.3.4.5.4.3.2.2.5.5.3.9.7.9.1.5.3.7.10.11.1.3.1.1.2.1.3.2.6.1.12.8.1.3.1.1.2.2.7.7.1.4.3.6.1.2.1.4.1.1.4.1.1.2.1.8.1.7.4.3.3.3.5.3.15.1.15.10.28.1.2.2.12.3.4.1.6.3.4.7.1.3.1.4.1.5.3.1.3.4.1.5.2.3.1.2.2.6.2.1.2.2.2.4.1.1.1.2.2.1.1.1.1.2.1.1.1.2.2.1.1.2.1.2.1.7.1.7.1.1.1.1.2.1.4.2.1.1.9.1.6.2.1.6.2.3.2.1.1.1.2.5.2.4.1.1.2.2.1.1.7.1.2.2.1.2.1.2.3.1.1.2.4.1.1.1.6.3.6.4.5.9.1.2.3.1.4.3.2.2.3.1.1.1.1.12.1.3.1.1.2.2.1.6.3.3.5.2.7.1.1.2.5.1.9.5.1.3.1.8.4.5.1.9.1.1.1.2.1.1.1.4.2.13.1.1.3.1.2.2.3.1.2.1.1.1.2.1.3.1.1.1.1.2.4.1.5.1.2.4.3.10.2.9.7.2.2.1.3.3.1.6.1.2.5.1.1.2.6.4.2.1.200.200.100.300.400.100.100.100.400.1700.304.596.100.1000.800.500.400.200.200.500.1300.801.99.303.203.95.1399.1100.100.4302.1798.2100.600.200.100.800.900.100.200.700.100.800.2000.900.1100.600.400.2200.2300.400.1101.899",
            "__gfp_64b": "uw9w_Zc.vDkgmbov5S7F6Z5C52.I7AVIs1qjzZlNNIb.e7|1739260500|2|||8:1:80",
            "_goa3": "eyJ1IjoiMjUwMjExMzE2MDk4NDgwMzM4NjkxNyIsImgiOiIxNTkuODkuMjA0LjE5MyIsInMiOjE3MzkyNjA0ODU2NDd9",
            "_goa3TS": "e30=",
            "___nrbic": "%7B%22isNewUser%22%3Atrue%2C%22previousVisit%22%3A1739260498%2C%22currentVisitStarted%22%3A1739260498%2C%22sessionId%22%3A%2225174ff9-fa5d-4eb6-96b5-a92d0f0cf942%22%2C%22sessionVars%22%3A%5B%5D%2C%22visitedInThisSession%22%3Atrue%2C%22pagesViewed%22%3A2%2C%22landingPage%22%3A%22https%3A//24.hu/%22%2C%22referrer%22%3A%22%22%2C%22lpti%22%3Anull%7D",
            "_goa3TC": "eyI3NzU0MjA5IjoxNzM5MjYwNTI2MjY0fQ==",
            "_pa_ut": "94461821-04f7-4a81-ad37-596ed71ed928",
            "__gads": "ID=385466c47955cfc4:T=1739260530:RT=1739260530:S=ALNI_MYF-rCy3EIbXgK6KXq5uaFijnmhtA",
            "__gpi": "UID=000010307c25c795:T=1739260530:RT=1739260530:S=ALNI_MaEcXAgZhFu4W_R9AtU3TIsiXcApg",
            "__eoi": "ID=5e5028248b818343:T=1739260530:RT=1739260530:S=AA-AfjZcMBEuS_a4FZNh_RJ6AXn7",
            "___nrbi": "%7B%22firstVisit%22%3A1739260498%2C%22userId%22%3A%226a847d96-f79d-4973-98f9-2aab6a366f00%22%2C%22userVars%22%3A%5B%5D%2C%22futurePreviousVisit%22%3A1739260498%2C%22timesVisited%22%3A1%2C%22userType%22%3A0%2C%22siteUserId%22%3A%220%22%2C%22userSegments%22%3A%5B%22front%22%2C%22news%22%5D%7D",
            "_goa3GDPR": "eyJnIjp0cnVlLCJjIjoiQ1FNcWI0QVFNcWI0QUFLQTFBSFVCY0ZzQVBfZ0FFUGdBQTZnS3p0WF9HX19iV2xyOFg3M2FmdGtlWTFQOTloNzdzUXhCaGJKRS00RnpMdldfSndYeDJFNU5BejZ0cUlLbVJJQXUzVEJJUU5sSEpEVVJWQ2dhb2dWclNETWFFeVVvVE5LSjZCa2lGTVJJMmRZQ0Z4dm00dGplUUNZNXZyOTkxZHgyQi10N2RyODNkenl5NGhIbjNhNV8yUzFXSkNkQTQtdERmdjliUk9iLTlJT2RfeDh2NHY0X0Y3cEUyX2VUMWxfdFd2cDdEOS1jdHNfOVhXOTlfZmJmZjlQbl8tdUJfLV9YX3ZmX0gzN3VDc2dCSmhvVkVBWlpFaElRYUJoQkFnQlVGWVFFVUNBSUFBRWdhSUNBRXdZRk9RTUFGMWhJZ0JBQ2dBR0NBRUFBSU1nQVFBQUNRQUlSQUJBQVVDQUFDQVFLQUFNQUNBWUNBQmdZQUF3QVdBZ0VBQUlEb0dLWUVFQWdXQUNSbVJRS1lFSUFDUVFFdGxRZ2tBUUlLNFFoRm5nRVFDSW1DZ0FBQkFBS1FBQkFXQ3dPSkpBU3NTQ0FMaUNhQUFBZ0FRQ0NBQW9SU2RtQUlJQXpaYXE4R1RhTXJUQXNIekJjOXBnR1FCRUVaT1NiRUFBLmQ5Z0FDQ0FBQUFBQSIsInQiOjE3MzkyNjA2MTg0NDd9",
            "_goa3B": "eyJjaHJvbWUiOnRydWUsInZlcnNpb24iOiIxMzIuMC4yOTU3LjExNSIsIndlYmtpdCI6dHJ1ZSwiZnVsbFZlcnNpb25zIjpbeyJicmFuZCI6Ik5vdCBBKEJyYW5kIiwidmVyc2lvbiI6IjguMC4wLjAifSx7ImJyYW5kIjoiQ2hyb21pdW0iLCJ2ZXJzaW9uIjoiMTMyLjAuNjgzNC44NCJ9LHsiYnJhbmQiOiJNaWNyb3NvZnQgRWRnZSIsInZlcnNpb24iOiIxMzIuMC4yOTU3LjExNSJ9XSwiYXJjaGl0ZWN0dXJlIjoieDg2IiwiYml0bmVzcyI6IjY0IiwibW9kZWwiOiIiLCJwbGF0Zm9ybSI6IldpbmRvd3MiLCJwbGF0Zm9ybVZlcnNpb24iOiIxMC4wLjAifQ==",
            "cto_bundle": "FbIyZF9PT1JQUkJIcFlMNFJhcloxd0pnNGdVZUhTTjhKWCUyRmZGOExIdlowMU1DJTJGVmNmS3pHdUVOJTJCeWNaVGU1ak1nWFc4TlllJTJCdHJXc2NNdW16b2RCTERFWENPYzByZ3hlJTJGQWswUkNwSUE5NFAzV3klMkJBVGd4ZTEwcFJaamVjYVRrekxTcFNPbm1rVmh2bTd4aE13REszeW5sWHclM0QlM0Q",
            "_ga_WQCPVWN1XZ": "GS1.1.1739260497.1.1.1739260694.0.0.0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        # print(type(response.text))
        # print(type(response.json))
        # links = json.loads(response.text)
        # print(response.text)
        # print(response.text)
        # print(response)
        links = response.xpath("//h3/a/@href").extract()

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
            items.table_name = self.table  # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url = item
            # items.title = item.get("headline")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://24.hu/page/{current_page}/"
        params = {
            "s": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, method='GET',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(
            response.xpath("//div[@class='u-onlyArticlePages']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
