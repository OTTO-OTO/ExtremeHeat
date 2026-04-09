# -*- coding: utf-8 -*-
"""

集群运行

"""
import base64
import re

import feapder
from feapder import Item
import json
from lxml import etree
from feapder.db.mysqldb import MysqlDB


class AirSpiderDemo(feapder.AirSpider):
    def __init__(self):
        super().__init__()
        self.previous_links = None  # 初始化 previous_links 为 None

    db = MysqlDB(
        ip="192.168.101.200", port=3307, db="spider_keywords", user_name="czm", user_pass="root"
    )
    sql = """ select db_name from keywords where country='Belgium' and language='英语'"""
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

    country = 'Belgium'
    table = 'Belgium'
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
            url = "https://www.standaard.be/graphql-blue-mhbe"
            params = {
                "variables": "{\"brand\":\"standaard.be\",\"count\":10,\"search\":\"hoge temperatuur\",\"publishedAfter\":\"\",\"publishedBefore\":\"\",\"sections\":[],\"ordering\":\"MOST_RECENT\",\"sourcesetCroppingInput\":{\"resizeMode\":\"SMART_CROP\",\"cropsMode\":null,\"fallbackResizeMode\":\"SMART_CROP\",\"sizes\":[{\"key\":\"xsmall\",\"width\":128,\"height\":80},{\"key\":\"small\",\"width\":160,\"height\":107},{\"key\":\"smallMobile\",\"width\":240,\"height\":160},{\"key\":\"medium\",\"width\":320,\"height\":213},{\"key\":\"large\",\"width\":640,\"height\":427},{\"key\":\"xlarge\",\"width\":960,\"height\":640},{\"key\":\"xxlarge\",\"width\":1280,\"height\":853}]},\"after\":null}",
                "operationName": "webv2_SearchArticlesConnection_ds_3_2",
                "persisted": "true",
                "extensions": "{\"persistedQuery\":{\"version\":1,\"sha256Hash\":\"18494a319630cc417373fd31920090f31199ed0368d9f6836caff9b44a983621\"}}"
            }
            variables = json.loads(params['variables'])

            # 更新 search 和 after 字段
            variables['search'] = f"{keyword}"
            variables['after'] = None  # 或根据需要设置为 None
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, method='GET',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "apollographql-client-name": "article-list-v2",
            "apollographql-client-version": "0.0.1246-ds",
            "priority": "u=1, i",
            "referer": "https://www.standaard.be/zoeken?keyword=extreme+temperaturen&daterange=all&datestart=&dateend=",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0",
            "x-client-name": "article-list-v2-ds"
        }
        request.cookies = {
            "auth0_1cZ7sPE6NJWugJLhO6VSxPli6jxe3pNT_config": "env=production&brand=ds&prefix=&version=v2",
            "auth0_1cZ7sPE6NJWugJLhO6VSxPli6jxe3pNT_silent_auth": "1739154843997",
            "_segmentgroup": "H",
            "_mhtc_cId": "21a8890b-f0ff-4178-a509-67f497a1eb80",
            "_impressionsegment": "[object Object]",
            "_gid": "GA1.2.1266492911.1739154962",
            "_mhtc_sId": "87fd7e65-4087-42d5-9e89-db13e43bd1fb",
            "didomi_token": "eyJ1c2VyX2lkIjoiMTk0ZWRiNTctNzRlYS02YmYxLTkwOGYtMGQ0YzVjNjgwNDJhIiwiY3JlYXRlZCI6IjIwMjUtMDItMTBUMDI6MzQ6NDAuMzM0WiIsInVwZGF0ZWQiOiIyMDI1LTAyLTEwVDAyOjM4OjA2LjMxMFoiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwidHdpdHRlciIsImM6YWRvYmUtdGFnbWFuYWdlciIsImM6YWdub3BsYXktTXpZY0Y0SEYiLCJjOnB1YnN0YWNrLWlZaGZRM2hRIiwiYzpmYWNlYm9vay1iajRGSkZIVyIsImM6b21uaXR1cmUtYWRvYmUtYW5hbHl0aWNzIiwiYzpyZWQtYnktc2ZyIiwiYzppbnRlbGxpYWQiLCJjOmltcGFjdC1yYWRpdXMiLCJjOnNoYXJwc3ByaW5nIiwiYzpiYXRjaC1qWFB4TU1MTiIsImM6Z29vZ2xlYW5hLW42VU1oSjdlIiwiYzpmYWNlYm9va3AtVURVOFlLTmYiLCJjOmdldHNpdGVjb24tOUNxekc3WjYiLCJjOmZyb29tbGUtNE56MlhEd04iLCJjOm9wdGltaXplbHktWXdWcTlNV2IiLCJjOnZ3by1pQ2U2MnhkNyIsImM6bWF0aGVyLWVjb25vbWljcyIsImM6aG90amFyLXluRjhtYVVSIiwiYzppb3RlY2hub2wteVlwY2Z0ZHoiLCJjOnR3aXR0ZXItM3czMzljTDYiLCJjOmFuYWxpZ2h0cy03bXlFSDNlayIsImM6Z29vZ2xlYW5hLTRUWG5KaWdSIiwiYzppbnN0YWdyYW0tZmhtcnhLTVciLCJjOm5leHRyb2xsLUdEbnBBREdiIiwiYzp6YWxhbmRvLXBUS1lWTWFiIiwiYzp2aXJ0dWFsbW4tRU1Bek1MRFciLCJjOnBtZy1aRTJDeUNGayIsImM6b21uaWNvbW1lLXlQaWo3Z1laIiwiYzppbnRlcnB1YmxpLThIWjhQZkczIiwiYzpnc2tpbm5lci1uVUUzNFAySCIsImM6dHdpcGUtQ2tpdE56WEQiLCJjOnplYmVzdG9mLWNkN05ZRUxMIiwiYzphZHNhbmRkYS16R1RHUlZIdyIsImM6Ymx1ZWNvbmljLW1mY2VQVVo5IiwiYzppbnN0YWdyYW0iLCJjOmZyYWN0aW9uYWwtbjJqWURGUE4iLCJjOmlnbml0aW9uby1MVkFNWmRuaiJdfSwicHVycG9zZXMiOnsiZW5hYmxlZCI6WyJzb2NpYWxfbWVkaWEiLCJ1aXRnZWJyZWlkLUFNTjJjaGV0Il19LCJ2ZXJzaW9uIjoyLCJhYyI6IkNnLUJDQUVZQUxJQVhRQTlBQ1ZBRS1BTVFBbTRCY3dEUGdIbUFQZUFqNEI2b0VHd0lrZ1MxQW40QlFvQ2lvRmh3TWJnWnpBMXlCMDREcXdIWVFRZWdpQkJHYUNkNEZCQUtEd0EuQUFBQSJ9",
            "euconsent-v2": "CQMnI8AQMnI8AAHABBENBbFgAP_gAAAAAAAAJOpH7GzVLWFC8G53YLsAMIxW5MAIAsQAAgaAA-ABiDqQMAQGkmAoNATgBAACABAAIDRBIAJFGAAQAUAAYIABAAAIQAQAAAJIICAAgAIAAEAAAAAKCoAAQAAIgEAAEAAAmQgAAJIAWFAAggAAIAAAAAAAAIAAAgAAAAAAAAAAAAAAACgAAAAAAAAAAAAAABAAAAAAAAAgAAAGKQAYAAgrKOgAwABBWUhABgACCspKADAAEFZQkAGAAIKyloAMAAQVlAAA.f_wAAAAAAAAA",
            "__gfp_64b": "syfV_aFk0A_v8YllyC.lRNypyJdi9fIH6y6AbMxy_9z.u7|1739155066|2|||8:1:80",
            "sc": "87fd7e65-4087-42d5-9e89-db13e43bd1fb.2",
            "_pctx": "%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAEzIEYOAWABgHYAzP14BOAKwAmUQDYp3ABy8ZIAL5A",
            "_pcid": "%7B%22browserId%22%3A%22m6yg4lr452xywt5h%22%7D",
            "cX_P": "m6yg4lr452xywt5h",
            "utag_main": "v_id:0194edb6b2da0012db83dae72fc80507d001907500bd0$_sn:1$_se:3$_ss:0$_st:1739157043024$ses_id:1739154961115%3Bexp-session$_pn:2%3Bexp-session",
            "_ga_9QPJK78BSN": "GS1.1.1739155243.1.0.1739155243.0.0.0",
            "_ga": "GA1.1.1717470071.1739154962",
            "_fbp": "fb.1.1739155243614.357932432414072145",
            "_ga_VCS5MLNYVP": "GS1.1.1739155243.1.0.1739155243.0.0.0",
            "_vwo_uuid_v2": "D3890CF958D5E621AE4483EA5E136F35A|323e3f170179636d2fecdd50ea9beeba",
            "_hjSessionUser_953": "eyJpZCI6ImVjODk4N2JiLTU5MGUtNTg4OS1iMTI1LTM4NDczOTY2ODhmOCIsImNyZWF0ZWQiOjE3MzkxNTUyNDQwOTAsImV4aXN0aW5nIjpmYWxzZX0=",
            "_hjSession_953": "eyJpZCI6IjU4NTNjYTIxLWNjYmEtNDZhYS05YWE5LTI4NDdkOTg4YzE3YyIsImMiOjE3MzkxNTUyNDQwOTEsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MH0=",
            "_sotmsid": "0:m6yg4mty:U8DSK6fj3jwmVt~gIL7CUkPqJwmvYcHc",
            "_sotmpid": "0:m6yg4mty:uCbQQt8DHNcOeTkHOjVTUkt3OfuYma1u",
            "cX_G": "cx%3A3ejadaumwr3mdx2pdfjl87pw2%3A3hx0q6qpfxxcq"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        next_id_str = response.json["extensions"]["cacheTags"][9]
        next_id = next_id_str.split(":")[-1]
        print(next_id, "*" * 20)
        links = response.json['data']['search'].get("edges")
        # print(links)
        endCursor = response.json['data']['search'].get("pageInfo").get("endCursor")
        hasNextPage = response.json['data']['search'].get("pageInfo").get("hasNextPage")
        if not hasNextPage:
            return None
        current_page = request.page

        current_links = links
        if self.previous_links is not None and current_links == self.previous_links:
            print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理

        request.previous_links = current_links  # 更新上一页的链接列表

        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        if request.page >= 10:
            print(f"关键词 {current_keyword} 的第 {request.page} 页已经抓取完毕，退出当前关键字的循环")
            return None
        for item in links:
            items = Item()
            items.table_name = self.table  # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url = "https://www.standaard.be" + item.get("node").get("relativeUrl")
            items.title = item.get("node").get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.standaard.be/graphql-blue-mhbe"
        params = {
            "variables": "{\"brand\":\"standaard.be\",\"count\":10,\"search\":\"hoge temperatuur\",\"publishedAfter\":\"\",\"publishedBefore\":\"\",\"sections\":[],\"ordering\":\"MOST_RECENT\",\"sourcesetCroppingInput\":{\"resizeMode\":\"SMART_CROP\",\"cropsMode\":null,\"fallbackResizeMode\":\"SMART_CROP\",\"sizes\":[{\"key\":\"xsmall\",\"width\":128,\"height\":80},{\"key\":\"small\",\"width\":160,\"height\":107},{\"key\":\"smallMobile\",\"width\":240,\"height\":160},{\"key\":\"medium\",\"width\":320,\"height\":213},{\"key\":\"large\",\"width\":640,\"height\":427},{\"key\":\"xlarge\",\"width\":960,\"height\":640},{\"key\":\"xxlarge\",\"width\":1280,\"height\":853}]},\"after\":null}",
            "operationName": "webv2_SearchArticlesConnection_ds_3_2",
            "persisted": "true",
            "extensions": "{\"persistedQuery\":{\"version\":1,\"sha256Hash\":\"18494a319630cc417373fd31920090f31199ed0368d9f6836caff9b44a983621\"}}"
        }
        variables = json.loads(params['variables'])

        # 更新 search 和 after 字段
        variables['search'] = f"{current_keyword}"
        variables['after'] = endCursor  # 或根据需要设置为 None
        print(params, "-" * 10)
        params['variables'] = json.dumps(variables)
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, method='GET',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        # items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@data-testid='article-body']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:modified_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
