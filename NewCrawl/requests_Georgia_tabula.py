import random
import time

from curl_cffi import requests
import pymysql
from lxml import etree


def insert_data(title, author, keyword, content, article_url, pubtime):
    try:
        # 在函数内部建立数据库连接
        db = pymysql.connect(
            host="192.168.101.200",
            port=3307,
            user="czm",
            password="root",
            database="spider_data"
        )
        cursor = db.cursor()

        # 插入数据的SQL语句
        insert_sql = """
        INSERT INTO Georgia_tabula (title, author, keyword, content, article_url, pubtime)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        # 要插入的数据
        data = (title, author, keyword, content, article_url, pubtime)
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
        "origin": "https://tabula.ge",
        "priority": "u=1, i",
        "referer": "https://tabula.ge/",
        "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
    }
    url = "https://api.tabula.ge/jsonapi/index/tabula_search_index"

    keywords =  [
    # 'ექსტრემალური',
    #     'გვარმი', 'მაღალი ტემპერატურა', 'მძიმე ტყვია', 'საშუალო',
    # 'გვარმის გამო ელექტროэнერგიის გამორთვა', 'მკვდარობა', 'აღ米rის დამჟავა', 'კლიმატის ცვლილება',
    # 'მთვრალი პირობების შემცირება', 'ოქსიგენის არ亚瑟', 'მაღალი ტემპერატურა ტრაფიკის გამო',
    # 'ეკოლოგიური დესტრუქცია', 'კლიმატის ცვliლება ეკoნოmიის გამო', 'ზღვის ჟარბი',
    # 'მაღალი ტემპერატურის დაmჟავა',
    #     'კორალები'
    ]
    # keywords = ['Heat','High Temperature','Heavy Rain','Drought','Power Outage from Heat','Fire','Air Pollution','Climate Change','Crop Yield Reduction','Oxygen Deficiency','High Temperature Affecting Traffic','Ecological Disaster','Climate Change Affecting Economy','Marine Heatwave','High Temperature Pollution','Coral']
    for keyword in keywords:
        page = 1
        while True:
            try:
                params = {
                    "filter[fulltext]": f"{keyword}",
                    "filter[search_api_language]": "ka",
                    "page[offset]": f"{page * 10}",
                    "page[limit]": "10",
                    "sort": "-search_api_relevance,-created"
                }
                response = requests.get(url, headers=headers, params=params, impersonate="safari15_3")
                links = response.json()['data']
                # print(links)
                print(f"当前是关键字{keyword}的第{page}页正在抓取中......")
                if not links:
                    break
                for item in links:
                    # print(links)
                    html = etree.HTML(item.get("attributes").get("body"))
                    title = item.get("attributes").get("title")
                    content = "".join(html.xpath("//p/text()"))
                    author = ''
                    article_url = item.get("links").get("self").get("href")
                    pubtime = ''
                    print(title, content)
                    insert_data(title, author, keyword, content, article_url, pubtime)
                    break
                page += 1
                time.sleep(random.randint(3, 5))
            except Exception as e:
                print(e)
                continue
        continue


if __name__ == '__main__':
    get_url()
