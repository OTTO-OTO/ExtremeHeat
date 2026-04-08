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
            database="TropicalCyclone"
        )
        cursor = db.cursor()

        # 插入数据的SQL语句
        insert_sql = """
        INSERT INTO Nigeria_channelstv (title, author, keyword, content, article_url, pubtime,country)
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
        "referer": "https://www.channelstv.com/?s=heavy+rain",
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
    cookies = {
        "_ga": "GA1.1.432381587.1738812615",
        "cf_clearance": "Rf0.y45dt8vz0djWtLIXj0IkXKqQv3pvrh.AYDlMkqE-1738812599-1.2.1.1-GyBGIUIsH9HppaYuyeZeTiazysbn5e4uFBiqiISBnXtdYyUo7OeyymftMZ6cxO5Q04QwtHlVxLCTTQKp0K3qBsK6SnlCryBKoaXNSNcAzhKyQhc81atpTqP3MxciVOvoPuzFFppZTn36GviRrJt3hIZUEZmjjR15Ro.Tas3h6tfFfU17MpkgTy9xA20VJQ6aupY6JjbkXyug41R3UnBMUHXUTOwYLow1QAtKALhDwYuywx3E0i4VAm3NCkmGvhG8HWxp4s6ZCdy9nu4fP3zCO9MYZJED9j7zF.Kn_.GM2Lc",
        "_ga_BXB3HDTB86": "GS1.1.1738812614.1.1.1738812816.59.0.0"
    }

    # 英语
    keywords = ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]
    for keyword in keywords:
        page = 1
        previous_links = []
        while True:
            try:
                url = f"https://www.channelstv.com/page/{page}/"
                params = {
                    "s": f"{keyword}"
                }
                response = requests.get(url, headers=headers, cookies=cookies, params=params,
                                        impersonate="chrome110").text
                html = etree.HTML(response)
                links = html.xpath("//article//h3/a/@href")
                print(links)
                print(f"当前是关键字{keyword}的第{page}页正在抓取中......")

                # 检查当前页的链接是否与前一页相同
                if set(links) == set(previous_links):
                    print(f"当前页与前一页的链接相同，退出关键词 {keyword} 的循环")
                    break

                previous_links = links  # 更新前一页的链接

                if not links:
                    print(f"关键词 {keyword} 的所有页面已抓取完毕")
                    break

                for link in links:
                    resp = requests.get(link, headers=headers, cookies=cookies, impersonate="chrome110").text
                    html2 = etree.HTML(resp)
                    title = html2.xpath("//h1/text()")[0]
                    content = "".join(html2.xpath("//div[@class='section-inner']//p/text()"))
                    author = ''
                    article_url = link
                    pubtime = html2.xpath("//meta[@property='article:published_time']/@content")[0]
                    country = 'Nigeria'
                    print(article_url)
                    print("title:", title)
                    print("content:", content)
                    insert_data(title, author, keyword, content, article_url, pubtime, country)

                page += 1
                time.sleep(random.randint(5, 8))
            except Exception as e:
                print(f"抓取过程中发生错误：{e}")
                continue
        continue


if __name__ == '__main__':
    get_url()
