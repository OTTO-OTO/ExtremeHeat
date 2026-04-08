import json
import random
import time

from curl_cffi import requests
import pymysql
from lxml import etree


def insert_data(title, author, keyword, content, article_url, pubtime, country):
    try:
        # 在函数内部建立数据库连接
        db = pymysql.connect(
            host="192.168.101.200",
            port=3307,
            user="czm",
            password="root",
            database="other_country_site3"
        )
        cursor = db.cursor()

        # 插入数据的SQL语句
        insert_sql = """
        INSERT INTO Nigeria_thenationonlineng (title, author, keyword, content, article_url, pubtime,country)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        # 要插入的数据
        data = (title, author, keyword, content, article_url, pubtime, country)
        # 执行SQL语句
        cursor.execute(insert_sql, data)
        # 提交事务
        db.commit()
        print("数据插入成功")
    except Exception as e:
        # 打印错误信息
        print(f"数据插入失败：{e}")
        # 回滚事务（如果有需要）
        db.rollback()
    finally:
        # 关闭游标和数据库连接
        cursor.close()
        db.close()


def get_url():
    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "referer": "https://thenationonlineng.net/",
        "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Microsoft Edge\";v=\"133\", \"Chromium\";v=\"133\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "script",
        "sec-fetch-mode": "no-cors",
        "sec-fetch-site": "cross-site",
        "sec-fetch-storage-access": "active",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0"
    }

    keywords = ["heatwave"]
    for keyword in keywords:
        page = 0
        previous_links = []
        while True:
            try:
                url = "https://cse.google.com/cse/element/v1"
                params = {
                    "rsz": "filtered_cse",
                    "num": "10",
                    "hl": "en",
                    "source": "gcsc",
                    "start": f"{page}",
                    "cselibv": "5c8d58cbdc1332a7",
                    "cx": "partner-pub-5089981496810613:3033809293",
                    "q": f"{keyword}",
                    "safe": "active",
                    "cse_tok": "AB-tC_4MYMOQvSYrHUlnRYeF60zL:1739754792723",
                    "lr": "",
                    "cr": "",
                    "gl": "",
                    "filter": "0",
                    "sort": "date",
                    "as_oq": "",
                    "as_sitesearch": "",
                    "exp": "cc",
                    "callback": f"google.search.cse.api19461",
                    "rurl": f"https://thenationonlineng.net/search-results/?q={keyword}"
                }
                response = requests.get(url, headers=headers, params=params, impersonate="chrome110").text
                links = json.loads(response.split(f"api19461(")[-1].split(");")[0])["results"]
                print(f"当前是关键字{keyword}的第{page}页正在抓取中......")

                if links == previous_links:
                    print(f"当前页与前一页的链接相同，退出关键词 {keyword} 的循环")
                    break

                previous_links = links  # 更新前一页的链接

                if not links:
                    print(f"关键词 {keyword} 的所有页面已抓取完毕")
                    break

                for item in links:
                    # title = item.get("title")
                    print(item)
                    article_url = item.get("url")
                    # print(article_url)
                    content_html = requests.get(article_url, headers=headers, impersonate="chrome110").text
                    html = etree.HTML(content_html)
                    content = "".join(html.xpath("//div[@class='article__content']//p//text()"))

                    author = ''
                    pubtime = html.xpath("//meta[@property='article:published_time']/@content")[0]
                    country = 'Nigeria'
                    title = html.xpath("//meta[@property='og:title']/@content")
                    # print(article_url)
                    # print("title:", title)
                    # print("content:", content)
                    # print("pubtime",pubtime)
                    # insert_data(title, author, keyword, content, article_url, pubtime, country)

                page += 10
                time.sleep(random.randint(5, 8))
            except Exception as e:
                print(f"抓取过程中发生错误：{e}")
                continue
        continue


if __name__ == '__main__':
    get_url()
