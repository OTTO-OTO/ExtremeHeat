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
            database="other_country_site2"
        )
        cursor = db.cursor()

        # 插入数据的SQL语句
        insert_sql = """
        INSERT INTO Dominica_dominicanewsonline (title, author, keyword, content, article_url, pubtime,country)
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
        "Referer": "https://dominicanewsonline.com/news/?s=heavy+rain",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
        "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\""
    }

    keywords =  ["heatwave","Extreme heatwave", "High temperature", "Extreme temperature", "Heatwave event", "Increase in high temperature", "High temperature impact", "Strong heat", "Temperature rise", "Heat event", "Temperature increase", "Heavy rainfall", "Heavy precipitation", "Torrential rain", "Extreme rainfall", "Drought", "Severe drought", "Long-term drought", "Water shortage", "Power outage", "High temperature power outage", "Heatwave power outage", "Power outage caused by high temperature", "Fire", "High temperature fire", "Heat fire", "Temperature fire", "Heat-induced fire", "Agricultural impact", "Heatwave agriculture", "Crop damage", "Agricultural heat stress", "Hypoxia", "Heatstroke", "Heat-induced heatstroke", "High temperature hypoxia", "High temperature heatstroke", "Traffic impact", "High temperature traffic", "Heatwave traffic", "Temperature traffic", "Ecological disaster", "Heat disaster", "High temperature environment", "Impact of heat on biodiversity", "Heatwave ecology", "Pollution", "High temperature pollution", "Heat pollution", "Temperature pollution", "Coral bleaching", "High temperature coral reefs", "Temperature coral bleaching" ]
    for keyword in keywords:
        page = 1
        previous_links = []
        while True:
            try:
                url = "https://dominicanewsonline.com/news/"
                params = {
                    "paged": f"{page}",
                    "s": f"{keyword}"
                }
                response = requests.get(url, headers=headers, params=params, impersonate="chrome110").text
                html = etree.HTML(response)
                links = html.xpath("//h2/a/@href")
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
                    resp = requests.get(link, headers=headers, impersonate="chrome110").text
                    html2 = etree.HTML(resp)
                    title = html2.xpath("//h1/text()")[0]
                    content = "".join(html2.xpath("//div[@class='entry-content']//p/text()"))
                    author = ''
                    article_url = link
                    pubtime = html2.xpath("//meta[@property='article:published_time']/@content")[0]
                    country = 'Dominica'
                    print(article_url)
                    print("title:", title)
                    print("content:", content)
                    print("pubtime:", pubtime)
                    # insert_data(title, author, keyword, content, article_url, pubtime, country)

                page += 1
                time.sleep(random.randint(3, 5))
            except Exception as e:
                print(f"抓取过程中发生错误：{e}")
                continue
        continue


if __name__ == '__main__':
    get_url()
