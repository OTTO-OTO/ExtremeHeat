import random
import time

from curl_cffi import requests
from lxml import etree
import pymysql

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
        INSERT INTO Antigua_and_Barbuda_antiguaobserver (title, author, keyword, content, article_url, pubtime)
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


def get_info():
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "priority": "u=0, i",
        "referer": "https://antiguaobserver.com/?s=heat",
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
        "_ga": "GA1.1.1337447330.1734401826",
        "sbjs_migrations": "1418474375998%3D1",
        "sbjs_current_add": "fd%3D2024-12-31%2002%3A11%3A20%7C%7C%7Cep%3Dhttps%3A%2F%2Fantiguaobserver.com%2F%7C%7C%7Crf%3D%28none%29",
        "sbjs_first_add": "fd%3D2024-12-31%2002%3A11%3A20%7C%7C%7Cep%3Dhttps%3A%2F%2Fantiguaobserver.com%2F%7C%7C%7Crf%3D%28none%29",
        "sbjs_current": "typ%3Dtypein%7C%7C%7Csrc%3D%28direct%29%7C%7C%7Cmdm%3D%28none%29%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29%7C%7C%7Cid%3D%28none%29%7C%7C%7Cplt%3D%28none%29%7C%7C%7Cfmt%3D%28none%29%7C%7C%7Ctct%3D%28none%29",
        "sbjs_first": "typ%3Dtypein%7C%7C%7Csrc%3D%28direct%29%7C%7C%7Cmdm%3D%28none%29%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29%7C%7C%7Cid%3D%28none%29%7C%7C%7Cplt%3D%28none%29%7C%7C%7Cfmt%3D%28none%29%7C%7C%7Ctct%3D%28none%29",
        "sbjs_udata": "vst%3D1%7C%7C%7Cuip%3D%28none%29%7C%7C%7Cuag%3DMozilla%2F5.0%20%28Windows%20NT%2010.0%3B%20Win64%3B%20x64%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F131.0.0.0%20Safari%2F537.36%20Edg%2F131.0.0.0",
        "cf_clearance": "H8yTBxZlLV1GdGiKATiv1Bh6swbkF3_iHIawZQSrwFw-1735611075-1.2.1.1-ztlWJYWk8e_00B3wflOQAwKAYrgCb9VKPu5x9o2e0x_eTxsHhEcW6Q.s8A7M_xBXwiz.B0vCg2i8HVcA72t3mV4NWC0ZQVhXbhSUguOIzgj5sk7z3Qbv0oI96iTZ7KaqcOBuIeJaBJkEa.CbCBFAc8JjKw8TYPc.51w9FiAubwCLAqpznze4AVZ4dPtRtpU3al6JOjp9kzFOwehESY669EcxOn1lz0fLQaNN2W8q.VSRBMrgAXN4Sd7HMrARUHDhx5hStZUhHJj3g2CdYYOJL_tqI2waVKjL1.ANW7Us41cpOfe7wsDBI3duoq9skBhymlLs_YgPbYrnOCHR6XMS0Ndvv3TkzdXv9wyzaBe7YdKS8m9My_GcWEvjbCXqfs.lE6hxUlCWxszCUQCuk9Qmm12SCmqO7_ilrIcmm0qR0D0",
        "__gads": "ID=1a6d9211100b7a92:T=1734401823:RT=1735611075:S=ALNI_MY5qd3xhOquGpI5rSQgSY_uAYJ7oA",
        "__gpi": "UID=00000f74451c054f:T=1734401823:RT=1735611075:S=ALNI_MYkOzU5G1J7g2IzP0Kkb8TH0V9Pdw",
        "__eoi": "ID=4c1da94a19f336ce:T=1734401823:RT=1735611075:S=AA-AfjZKUo-I2bxVkvY5h9R4SFsa",
        "_ga_VCTJXX63EM": "GS1.1.1735611080.5.1.1735611143.60.0.0",
        "sbjs_session": "pgs%3D3%7C%7C%7Ccpg%3Dhttps%3A%2F%2Fantiguaobserver.com%2F%3Fs%3Dheat",
        "FCNEC": "%5B%5B%22AKsRol_5U9CWyWb8ykHFK_efSSUHG7m5mWUUz-LW_eYcHijEPQ0Ypjx9q7IngzoNO2G5Nryn5FkJQE7kJwT34j-qP2r-2JeukM2JyX0JpGPleaxw6IgntoOi1XRmGi7j-O9lLbPj9Ydz0likDM5ZRcxwhk87UlmaPg%3D%3D%22%5D%5D"
    }
    keywords = ['Heat','High Temperature','Heavy Rain','Drought','Power Outage from Heat','Fire','Air Pollution','Climate Change','Crop Yield Reduction','Oxygen Deficiency','High Temperature Affecting Traffic','Ecological Disaster','Climate Change Affecting Economy','Marine Heatwave','High Temperature Pollution','Coral']
    for keyword in keywords:
        page = 1
        while True:
            try:
                url = f"https://antiguaobserver.com/page/{page}/"
                params = {
                    "s": f"{keyword}"
                }
                response = requests.get(url, headers=headers, params=params,cookies=cookies, impersonate="chrome110").text
                print(response)
                html = etree.HTML(response)
                print(f"当前关键字{keyword}的第{page}页正在抓取.......")
                links = html.xpath("//h3/a/@href")[:10]
                if not links:
                    break
                for link in links:
                    resp = requests.get(link, headers=headers, cookies=cookies, impersonate="chrome110").text
                    html2 = etree.HTML(resp)
                    title = html2.xpath("//h1/text()")[0]
                    content = "".join(html2.xpath("//div[@class='td-post-content tagdiv-type']//p/text()"))
                    author = ''
                    article_url = link
                    pubtime = ''
                    if content:
                        insert_data(title, author, keyword, content, article_url, pubtime)
                page += 1
                time.sleep(random.randint(5,8))
            except Exception as e:
                print(e)
                continue
        continue

if __name__ == '__main__':
    get_info()
