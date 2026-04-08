import random
import time

from curl_cffi import requests
import pymysql
from lxml import etree


def insert_data(title, author, keyword, content, article_url, pubtime,country):
    try:
        # 在函数内部建立数据库连接
        db = pymysql.connect(
            host="192.168.101.200",
            port=3307,
            user="czm",
            password="root",
            database="other_country_site1"
        )
        cursor = db.cursor()

        # 插入数据的SQL语句
        insert_sql = """
        INSERT INTO Kazakhstan_kazinform (title, author, keyword, content, article_url, pubtime,country)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        # 要插入的数据
        data = (title, author, keyword, content, article_url, pubtime,country)
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
        "referer": "https://en.inform.kz/search_results/?q=Heavy+rain",
        "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
    }
    cookies = {
        "_pk_id.1.8d8f": "a738a12f01684b8c.1735961479.",
        "_ga": "GA1.1.2074121061.1735961479",
        "_ym_uid": "173596148046658791",
        "_ym_d": "1735961480",
        "_ym_isad": "2",
        "_pk_ses.1.8d8f": "1",
        "_ga_Y44SMVECD5": "GS1.1.1735969118.2.1.1735969368.20.0.2031119465"
    }

    keywords = [
        # 'Extreme',
        #        'Heat',
        'High Temperature','Heavy Rain','Drought','Power Outage from Heat','Fire','Air Pollution','Climate Change','Crop Yield Reduction','Oxygen Deficiency','High Temperature Affecting Traffic','Ecological Disaster','Climate Change Affecting Economy','Marine Heatwave','High Temperature Pollution','Coral'
                ]
    # keywords = ['Air Pollution','Climate Change','Crop Yield Reduction','Oxygen Deficiency','High Temperature Affecting Traffic','Ecological Disaster','Climate Change Affecting Economy','Marine Heatwave','High Temperature Pollution','Coral']
    # keywords = ['heavy rain']
    for keyword in keywords:
        page = 1
        while True:
            try:
                url = "https://en.inform.kz/search_results/"
                params = {
                    "q": f"{keyword}",
                    "page": f"{page}"
                }
                response = requests.get(url, headers=headers, cookies=cookies, params=params,impersonate="chrome110").text
                html = etree.HTML(response)
                links = html.xpath("//div[@class='searchCard']/a/@href")
                print(links)
                print(f"当前是关键字{keyword}的第{page}页正在抓取中......")
                if not links:
                    break
                for link in links:
                    # print(links)
                    resp = requests.get("https://en.inform.kz"+link, headers=headers, cookies=cookies, impersonate="chrome110").text
                    html2 = etree.HTML(resp)
                    title = html2.xpath("//title/text()")[0]
                    content = "".join(html2.xpath("//div[@class='article__body']//div//p/text()"))
                    author = ''
                    article_url ="https://en.inform.kz"+link
                    pubtime = html2.xpath("//meta[@name='article:published_time']/@content")[0]
                    country = 'Kazakhstan'
                    print(title)
                    print(content)
                    print(article_url)
                    print(pubtime)
                    if not insert_data(title, author, keyword, content, article_url, pubtime, country):
                        print(f"数据插入失败，退出当前关键词 {keyword} 的循环")
                        break  # 退出当前for循环

                page += 1
                time.sleep(random.randint(3, 5))
            except Exception as e:
                print(e)
                continue
        continue


if __name__ == '__main__':
    get_url()
