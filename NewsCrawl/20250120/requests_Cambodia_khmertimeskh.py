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
        INSERT INTO Cambodia_khmertimeskh (title, author, keyword, content, article_url, pubtime,country)
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
        "referer": "https://www.khmertimeskh.com/?s=heavy+rain",
        "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
        "sec-ch-ua-arch": "\"x86\"",
        "sec-ch-ua-bitness": "\"64\"",
        "sec-ch-ua-full-version": "\"131.0.2903.146\"",
        "sec-ch-ua-full-version-list": "\"Microsoft Edge\";v=\"131.0.2903.146\", \"Chromium\";v=\"131.0.6778.265\", \"Not_A Brand\";v=\"24.0.0.0\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-model": "\"\"",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-ch-ua-platform-version": "\"10.0.0\"",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
    }
    cookies = {
        "_ga": "GA1.1.1770631110.1737359404",
        "cf_clearance": "ZW7dhE6FVpFXC4yIeZkfh0h4GWXzD0WBB1Klj1GC7XQ-1737359390-1.2.1.1-crhl.h1V5PR5_pli.TW7j67UihVXyqeVM4ppvWtsOzjcWBpfC.1DKc50DPjzxJdwL1HFnTK4BUW.QQcKzbel5hJWrUxw989Nw4lzqzgGRn_QaNjbgaPiE.fArWqJ83FwikXbRGbQ7ma76ltcID7n7F3JgWEWdjpRUTVv6AnhN5iNoob2l2.V4a3c_HxjVCWdhPk2WFd1gN1yi_3Idc6oFjY98OSHv4KM6X072_0.q11AbVogvIbSqUaHhBZrwYsnyP6R0f2eg6kXFkGfmZEZnn9seEbJloL_RM1cPXQNjm6nwXM2iuFuTmMphpr6qk6M_X7dYM1h9KU9ER.g2tNz4A",
        "__gads": "ID=60701a28b332ae72:T=1737359392:RT=1737359392:S=ALNI_MbDMQYw1XFin6ORHkytESkd0nfpbg",
        "__gpi": "UID=00000ff32d4ce371:T=1737359392:RT=1737359392:S=ALNI_MZjV07NDgUomByUadQZVYZ9bCOCog",
        "__eoi": "ID=401b1a5487d9c68d:T=1737359392:RT=1737359392:S=AA-AfjbX6IiFSzaWUuVdjqXJw7pD",
        "yt_scase_pro_wp_session": "a0ebb973688a8f93a8a24bb14c836a0a%7C%7C1737361351%7C%7C1737360991",
        "_ga_FRR2MK78XG": "GS1.1.1737359403.1.1.1737359595.19.0.0",
        "FCNEC": "%5B%5B%22AKsRol8ZRYCZFRAJFIw3cmylM6IYcqMOIjWHeCcJ5vgc4Hu3VrwauFjjZaFtC8IB7Jzh-uEvXs8uS9XadwM9iSJBrgVFAo_jREyTNTU1bmjQv7qU6pwzooBMhMhFc_DxlB-ddgTfbkrZ9fHVpJUrOPn2bYjA1o2Z6g%3D%3D%22%5D%5D"
    }

    keywords = ["heatwave","Extreme heatwave", "High temperature", "Extreme temperature", "Heatwave event", "Increase in high temperature", "High temperature impact", "Strong heat", "Temperature rise", "Heat event", "Temperature increase", "Heavy rainfall", "Heavy precipitation", "Torrential rain", "Extreme rainfall", "Drought", "Severe drought", "Long-term drought", "Water shortage", "Power outage", "High temperature power outage", "Heatwave power outage", "Power outage caused by high temperature", "Fire", "High temperature fire", "Heat fire", "Temperature fire", "Heat-induced fire", "Agricultural impact", "Heatwave agriculture", "Crop damage", "Agricultural heat stress", "Hypoxia", "Heatstroke", "Heat-induced heatstroke", "High temperature hypoxia", "High temperature heatstroke", "Traffic impact", "High temperature traffic", "Heatwave traffic", "Temperature traffic", "Ecological disaster", "Heat disaster", "High temperature environment", "Impact of heat on biodiversity", "Heatwave ecology", "Pollution", "High temperature pollution", "Heat pollution", "Temperature pollution", "Coral bleaching", "High temperature coral reefs", "Temperature coral bleaching" ]
    for keyword in keywords:
        page = 1
        while True:
            try:
                url = f"https://www.khmertimeskh.com/page/{page}/"
                params = {
                    "s": f"{keyword}"
                }
                response = requests.get(url,params=params, headers=headers,cookies=cookies,
                                        impersonate="chrome110").text
                html = etree.HTML(response)
                links = html.xpath("//article//h2/a/@href")
                print(links)
                print(f"当前是关键字{keyword}的第{page}页正在抓取中......")
                if not links:
                    break
                for link in links:
                    # print(links)
                    resp = requests.get(link, headers=headers,cookies=cookies,impersonate="chrome110").text
                    html2 = etree.HTML(resp)
                    title = html2.xpath("//title/text()")[0]
                    content = "".join(html2.xpath("//div[@class='entry-content']//p/text()"))
                    author = ''
                    article_url = link
                    pubtime = ''
                    country = 'Cambodia'
                    print(article_url)
                    print("title:",title)
                    print("content:",content)
                    # 调用 insert_data 并根据返回值判断是否退出循环
                    # success = insert_data(title, author, keyword, content, article_url, pubtime, country)
                    # if not success:
                    #     print(f"数据插入失败，退出当前关键词 {keyword} 的循环")
                    #     print("即将执行 break，退出当前 for 循环")
                    #     break  # 退出当前 for 循环
                    # else:
                    #     print("数据插入成功，继续处理下一个链接")

                page += 1
                time.sleep(random.randint(3, 5))
            except Exception as e:
                print(e)
                continue
        continue


if __name__ == '__main__':
    get_url()
