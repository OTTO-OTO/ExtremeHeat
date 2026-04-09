import json
import random
import time
from curl_cffi import requests
import pymysql
from lxml import etree
from pymysql import IntegrityError


def connect_to_database():
    """建立数据库连接并返回连接对象"""
    try:
        db = pymysql.connect(
            host="192.168.101.200",
            port=3307,
            user="czm",
            password="root",
            database="spider_keywords"
        )
        print("数据库连接成功")
        return db
    except Exception as e:
        print(f"数据库连接失败：{e}")
        return None

def insert_data(title, author, keyword, content, article_url, pubtime, country):
    try:
        db = connect_to_database()
        table = 'Georgia'

        cursor = db.cursor()
        cursor.execute(""" select db_name from keywords where country='Georgia' and language = '格鲁吉亚语'""")
        mysql_db = cursor.fetchall()[0][0]
        print("待写入的数据库是==========>",mysql_db)
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{mysql_db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")

        db2 = pymysql.connect(
            host="192.168.101.200",
            port=3307,
            user="czm",
            password="root",
            database=f"{mysql_db}"
        )
        cursor2 = db2.cursor()
        print("待写入的数据库是==========>", mysql_db)# 判断数据库是否存在

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
        cursor2.execute(create_table_sql)

        insert_sql = f"""
        INSERT INTO {table}(title, author, keyword, content, article_url, pubtime, country)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        data = (title, author, keyword, content, article_url, pubtime, country)
        cursor2.execute(insert_sql, data)
        db2.commit()
        print("数据插入成功")
        return True
    except IntegrityError as e:
        print(f"重复数据跳过：{e}")
        db.rollback()
        return False
    except Exception as e:
        print(f"数据插入失败：{e}")
        db.rollback()
        return False
    finally:
        cursor.close()
        db.close()


def get_url():
    country = "Georgia"
    table = 'Georgia'

    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "origin": "https://tabula.ge",
        "priority": "u=1, i",
        "referer": "https://tabula.ge/",
        "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Microsoft Edge\";v=\"133\", \"Chromium\";v=\"133\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0"
    }
    db = connect_to_database()
    cursor = db.cursor()

    #格鲁吉亚语
    cursor.execute(f"select keywords_list from keywords where language = '格鲁吉亚语' and country='{table}'")
    keywords = json.loads(cursor.fetchone()[0])
    # keywords = ['ტაიფუნი']
    print(keywords)
    processed_urls = set()

    for keyword in keywords:
        page = 0
        duplicate_count = 0
        stop_processing = False
        previous_links = []  # 初始化 previous_links

        while not stop_processing:
            try:
                print(f"当前是关键字 {keyword} 的第 {page+1} 页正在抓取中...")
                url = "https://api.tabula.ge/jsonapi/index/tabula_search_index"
                params = {
                    "filter[fulltext]": f"{keyword}",
                    "filter[search_api_language]": "ka",
                    "page[offset]": f"{page}",
                    "page[limit]": "10",
                    "sort": "-search_api_relevance,-created"
                }

                response = requests.get(
                    url=url,
                    headers=headers,
                    params=params,
                    impersonate="chrome110"
                ).json()
                # print(response)
                links = response.get('data', [])
                print(f"当前页面链接数量：{len(links)}")
                # 终止条件1：没有新数据
                if not links:
                    print(f"关键词 {keyword} 的所有页面已抓取完毕")
                    break

                # 终止条件2：连续两页出现重复链接
                if links == previous_links:
                    print(f"连续两页出现相同结果，终止抓取关键词 {keyword}")
                    break

                # 处理当前页数据
                for link in links:
                    article_url = link.get("attributes").get("legacy_url")

                    if article_url in processed_urls:
                        print(f"发现重复链接：{article_url}")
                        duplicate_count += 1
                        if duplicate_count >= 5:
                            print("连续出现多个重复链接，终止当前关键词处理")
                            stop_processing = True
                            break
                        continue

                    processed_urls.add(article_url)
                    duplicate_count = 0

                    title = link.get("attributes").get("title")
                    html = etree.HTML(link.get("attributes").get('body'))
                    content = "".join(html.xpath("//text()"))
                    pubtime =link.get('attributes').get("created")
                    print(title)
                    print(content)
                    if content:
                        success = insert_data(
                            title=title,
                            author="",
                            keyword=keyword,
                            content=content,
                            article_url=article_url,
                            pubtime=pubtime,
                            country=country
                        )

                        if not success:
                            print("检测到重复数据或插入失败，终止当前关键词处理")
                            stop_processing = True
                            break

                previous_links = links.copy()
                page +=10
                time.sleep(random.randint(5, 6))

            except Exception as e:
                print(f"抓取过程中发生错误：{e}")
                time.sleep(6)
                continue

        print(f"已完成关键词 {keyword} 的抓取\n")


if __name__ == '__main__':
    get_url()