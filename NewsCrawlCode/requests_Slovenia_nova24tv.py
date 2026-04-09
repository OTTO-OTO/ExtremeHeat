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
        INSERT INTO Slovenia_nova24tv (title, author, keyword, content, article_url, pubtime,country)
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
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "priority": "u=0, i",
        "referer": "https://nova24tv.si/?s=visoka+temperatura",
        "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Microsoft Edge\";v=\"133\", \"Chromium\";v=\"133\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0"
    }

    keywords = [
    "toplotna vala", "ekstremne toplotne vala", "visoka temperatura", "ekstremne temperature", "toplotevni dogodek",
    "povečanje temperature", "vpliv visoke temperature", "visoka temperatura", "močna toplota", "porast temperature",
    "toplinski dogodek", "naraščanje temperature", "močna padavina", "močna nizba", "liva", "ekstremna padavina",
    "suša", "težka suša", "dolgotrajna suša", "manjka vodnih virov", "izklop elektrike", "izklop elektrike zaradi tople",
    "izklop elektrike zaradi toplotne vala", "izklop elektrike zaradi visoke temperature", "požar", "požar zaradi visoke temperature",
    "toplinski požar", "požar zaradi temperature", "požar izražen zaradi temperature", "vpliv na kmetijstvo", "toplotevni val v kmetijstvu",
    "škoda na kmetijskih rastlinah", "toplinski stresek v kmetijstvu", "hipoksija", "toplinski udar", "toplinski udar zaradi visoke temperature",
    "toplinski udar zaradi visoke temperature", "vpliv na promet", "promet zaradi visoke temperature", "promet zaradi toplotne vala",
    "promet zaradi temperature", "ekološka katastrofa", "toplinska katastrofa", "toplinski okolje", "vpliv topline na biotsko raznolikost",
    "toplotevni val v ekologiji", "zagađenje", "zagađenje zaradi visoke temperature", "toplinska zagađenje", "zagađenje temperature",
    "bijelojanje koraljev", "visoka temperatura koralnih rifa", "bijelojanje koraljev zaradi temperature"
]
    for keyword in keywords:
        page = 0
        previous_links = []
        while True:
            try:
                url = f"https://nova24tv.si/page/{page}/"
                params = {
                    "s": f"{keyword}"
                }
                response = requests.get(url, headers=headers, params=params, impersonate="chrome110").text
                links = etree.HTML(response).xpath("//h3/a/@href")
                print(f"当前是关键字{keyword}的第{page}页正在抓取中......")

                if links == previous_links:
                    print(f"当前页与前一页的链接相同，退出关键词 {keyword} 的循环")
                    break

                previous_links = links  # 更新前一页的链接

                if not links:
                    print(f"关键词 {keyword} 的所有页面已抓取完毕")
                    break

                print(links)
                for item in links:
                    # print(item)
                    html_str = requests.get(item, headers=headers, impersonate="chrome110").text
                    html = etree.HTML(html_str)
                    content = "".join(html.xpath("//div[@class='tdb-block-inner td-fix-index']//p/text()"))
                    article_url = item
                    author = ''
                    title = html.xpath("//meta[@property='og:title']/@content")[0]
                    pubtime = html.xpath("//meta[@property='article:published_time']/@content")[0]
                    country = 'Slovenia'
                    print(article_url)
                    print("title:", title)
                    print("content:", content)
                    print("pubtime",pubtime)

                    insert_data(title, author, keyword, content, article_url, pubtime, country)

                page += 1
                time.sleep(random.randint(5, 8))
            except Exception as e:
                print(f"抓取过程中发生错误：{e}")
                continue
        continue


if __name__ == '__main__':
    get_url()
