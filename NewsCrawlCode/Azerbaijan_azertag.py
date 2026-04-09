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
        table = 'Azerbaijan'

        cursor = db.cursor()
        cursor.execute(""" select db_name from keywords where country='Azerbaijan' and language='英语'""")
        mysql_db = cursor.fetchall()[0][0]
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{mysql_db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")

        db2 = pymysql.connect(
            host="192.168.101.200",
            port=3307,
            user="czm",
            password="root",
            database=f"{mysql_db}"
        )
        cursor2 = db2.cursor()
        print("待写入的数据库是:", mysql_db)# 判断数据库是否存在

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
    country = "Azerbaijan"
    table = 'Azerbaijan'

    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "priority": "u=0, i",
        "referer": "https://azertag.az/en/axtarish?search=heavy+rain",
        "sec-ch-ua": "\"Microsoft Edge\";v=\"135\", \"Not-A.Brand\";v=\"8\", \"Chromium\";v=\"135\"",
        "sec-ch-ua-arch": "\"x86\"",
        "sec-ch-ua-bitness": "\"64\"",
        "sec-ch-ua-full-version": "\"135.0.3179.85\"",
        "sec-ch-ua-full-version-list": "\"Microsoft Edge\";v=\"135.0.3179.85\", \"Not-A.Brand\";v=\"8.0.0.0\", \"Chromium\";v=\"135.0.7049.96\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-model": "\"\"",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-ch-ua-platform-version": "\"10.0.0\"",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0"
    }
    cookies = {
        "_ym_uid": "173442118552137668",
        "_ym_d": "1739339362",
        # "cf_clearance": "yVyMForbIytBD.vfwXwQhmzSIAJQHELOsPxSGfcKsh4-1743217766-1.2.1.1-qsNp_lBkjClQUEYsiLrZ9UKRResbK7GrDSYAGuM36EaHEhErlaRbZ4Zy.XuSlqgs5nTy75NS11_aJW360_IGjyitHOHW.8Sr4aOc0FAP4K_MDOx5fDL0mSfJTtQKzs.a9l1ZfYhHPdscHGzYn5LcGaSNsww2DtfHMhFfx7xMvhpguPCohm5_QBfqzLOLehHk1J_bwD1G4ZdVpcghtdC..9DcHVz30tXrvCYQvUA29M0PXBy67Tsr48qj6P2oe.6d6kA_N.3kQISyKUBBnPB3nBeC9OVTSeOi53P3uso4qyXFgnqlc89wbpzWDwo8uZMcik_yLlIZyUGsh1lbz6wbns4cJvZD5osbTyqJENhM_LVHnIdq5Xb_1qzvbZYe2q_gOdC7RREIyTzhhrL9gqOQIK3Lo_QGg6uNSJ4vl.ds8Po",
        "PHPSESSID": "lo5o8ceoh5ngoglfj5ad2as6cn",
        "_gid": "GA1.2.1293467907.1745646798",
        "_gat_gtag_UA_26624560_2": "1",
        "_ym_isad": "2",
        "_ym_visorc": "b",
        "_ga": "GA1.1.1617855881.1739339361",
        "_ga_XR59NWGS41": "GS1.1.1745646798.5.1.1745646830.28.0.0"
    }
    db = connect_to_database()
    cursor = db.cursor()

    cursor.execute(f"select keywords_list from keywords where language = '英语' and country='{table}'")
    keywords = json.loads(cursor.fetchone()[0])
    print(keywords)
    processed_urls = set()

    for keyword in keywords:
        page = 1
        duplicate_count = 0
        stop_processing = False
        previous_links = []  # 初始化 previous_links

        while not stop_processing:
            try:
                print(f"当前是关键字 {keyword} 的第 {page+1} 页正在抓取中...")
                url = "https://azertag.az/en/axtarish"
                params = {
                    "search": f"{keyword}",
                    "page": f"{page}"
                }

                response = requests.get(
                    url=url,
                    cookies=cookies,
                    headers=headers,
                    params=params,
                    impersonate="chrome110"
                ).text

                html = etree.HTML(response)
                links = html.xpath("//h2/a/@href")

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
                    article_url = "https://azertag.az" + link

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

                    html = etree.HTML(requests.get(article_url, headers=headers).text)

                    title = html.xpath("//title/text()")[0]
                    content = "".join(html.xpath("//div[@class='news-body']//p/text()"))
                    pubtime = html.xpath("//ul[@class='global-list']/li[1]/text()")[0]
                    print(title)
                    print(content)
                    print(pubtime)
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
                page +=1
                time.sleep(random.randint(5, 6))

            except Exception as e:
                print(f"抓取过程中发生错误：{e}")
                time.sleep(6)
                continue

        print(f"已完成关键词 {keyword} 的抓取\n")


if __name__ == '__main__':
    get_url()