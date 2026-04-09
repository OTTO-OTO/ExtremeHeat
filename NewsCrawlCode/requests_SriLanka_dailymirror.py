import random
import time
from curl_cffi import requests
import pymysql
from lxml import etree
from pymysql import IntegrityError


def insert_data(title, author, keyword, content, article_url, pubtime, country):
    try:
        db = pymysql.connect(
            host="192.168.101.200",
            port=3307,
            user="czm",
            password="root",
            database="TropicalCyclone"
        )
        cursor = db.cursor()

        insert_sql = """
        INSERT INTO Sri_Lanka(title, author, keyword, content, article_url, pubtime, country)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        data = (title, author, keyword, content, article_url, pubtime, country)
        cursor.execute(insert_sql, data)
        db.commit()
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
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "content-type": "application/x-www-form-urlencoded",
        "origin": "https://www.dailymirror.lk",
        "priority": "u=1, i",
        "referer": "https://www.dailymirror.lk/search",
        "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0"
    }
    cookies = {
        "nsd": "0",
        "mobile": "0",
        "_gid": "GA1.2.482186598.1738910794",
        "cf_clearance": "L7MzfQncq7JlpBbJfWxZJwoLbA8ES93kcNXFPag7jm4-1738910774-1.2.1.1-7VLQr4fLSlMP5gCqPSK7Ipgn3Cj2x9LnOYjHLPjXCiiWQiRFwAdlLGh3pcazWokvL68RJVnCWTGP.yRvrGexceatRVKhkkrxWRe0xleP81CVH8PhM0Ch7Bqb.95QBOIUNEbpHiexmqd2q2NC62yno3rteYIgu7RgTVM1lifMOXNqBqe555ObQ6T9j8huBq7PK.YQvSUhvfwt4lCvuVV8ZVuYx_XQ.BkY0eGtW4mbYgCFDJtTqFsTLR3r2Y5PkCaAILbb37pKYZfm6fSt1zbBxSpVvI_w69xAxFN9BG4aO3g",
        "ci_session": "a6aj5jjh5f68sbj7na0ggr0s8mruln4f",
        "_ga": "GA1.2.1816977781.1738910790",
        "_ga_MKM4WL20FT": "GS1.1.1738910789.1.1.1738910870.39.0.0"
    }

    keywords = [
        "Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone",
        "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster",
        "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"
    ]

    processed_urls = set()

    for keyword in keywords:
        page = 0
        duplicate_count = 0
        stop_processing = False
        previous_links = []  # 初始化 previous_links

        while not stop_processing:
            try:
                print(f"当前是关键字 {keyword} 的第 {page // 30 + 1} 页正在抓取中...")

                data = {
                    "category": "0",
                    "order": "DESC",
                    "datefrom": "0",
                    "dateto": "0",
                    "start": str(page),
                    "keywords": keyword
                }

                response = requests.post(
                    "https://www.dailymirror.lk/home/search",
                    headers=headers,
                    cookies=cookies,
                    data=data,
                    impersonate="chrome110"
                ).json()

                links = response.get('articles', [])

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
                    article_url = f"https://www.dailymirror.lk/hard-talk/{link.get('ALIAS')}/{link.get('AUTHOR')}-{link.get('ARTICLE_ID')}"

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

                    title = link.get("TITLE", "无标题")
                    content_html = link.get("FULL_TEXT", "")
                    html = etree.HTML(content_html)
                    content = "".join(html.xpath("//p//text()")) if content_html else ""
                    pubtime =link.get('PUBLISH_DATE')
                    if content:
                        success = insert_data(
                            title=title,
                            author="",
                            keyword=keyword,
                            content=content,
                            article_url=article_url,
                            pubtime=pubtime,
                            country="Sri Lanka"
                        )

                        if not success:
                            print("检测到重复数据或插入失败，终止当前关键词处理")
                            stop_processing = True
                            break

                previous_links = links.copy()
                page += 30
                time.sleep(random.randint(5, 8))

            except Exception as e:
                print(f"抓取过程中发生错误：{e}")
                time.sleep(10)
                continue

        print(f"已完成关键词 {keyword} 的抓取\n")


if __name__ == '__main__':
    get_url()