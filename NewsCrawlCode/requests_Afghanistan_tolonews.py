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
        INSERT INTO Afghanistan_tolonews (title, author, keyword, content, article_url, pubtime)
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
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "origin": "https://tolonews.com",
        "priority": "u=1, i",
        "referer": "https://tolonews.com/search?search=heavy+rain",
        "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
        "x-requested-with": "XMLHttpRequest"
    }
    cookies = {
        "_ga": "GA1.2.68306338.1734332244",
        "fpestid": "DDpC7aXwScceK5oRghoHcwchECBB2x6ac1UHk4R__sbfBd_L1VV0Sqznv-1XE6bOjOqFpA",
        "_gid": "GA1.2.696166561.1735549048",
        "cf_clearance": "T8NxYe.iGlh6iL4ogUNGLvxskyl6_jf2Rptgl4bZi54-1735606828-1.2.1.1-B9Ir4GbYpGEQrNpUQ7_h9HAB2PhADuNiSnNOCY07zIzIah.8Fxzs1hiM82gY40dBeymTRMC_lKxx3NGlV_491PG7G.cbrn0H2kXkVi4cM2rkctVZvOEfceo3D0EoE6_tFxhywT1lljTKENp7fC8PRBNbkg_NP7PfzzBI2qUCJTzhTTpeHm57MjRNrsz0vukVfRrxVSDY5BucjM.SaMgMjhXyjoS1rdbikdqe9U0TSHmiaA.Rm6bRDsTPOD2hkVJkhXGwhkdDdVRoaWNE6ANqh9qPzdDASRuRCjxr5TeCBaC4FLEfM4rXnNtQrVlw2HOrjM9R1SFa4CWTz7aSDO.q6zYLRX4r5n1RVGTTbxqyYLUzqngLTJyKnj1qvYtc8OeUo5f4WO5jWJWrxZO8CZ1655XFK2ePp7VbCGiKnqA_nBU",
        "_ga_QVGK9W22XK": "GS1.2.1735606834.6.1.1735607047.28.0.0"
    }
    url = "https://tolonews.com/views/ajax"

    keywords = ['Extreme','Heat','High Temperature','Heavy Rain','Drought','Power Outage from Heat','Fire','Air Pollution','Climate Change','Crop Yield Reduction','Oxygen Deficiency','High Temperature Affecting Traffic','Ecological Disaster','Climate Change Affecting Economy','Marine Heatwave','High Temperature Pollution','Coral']
    # keywords = ['Heat','High Temperature','Heavy Rain','Drought','Power Outage from Heat','Fire','Air Pollution','Climate Change','Crop Yield Reduction','Oxygen Deficiency','High Temperature Affecting Traffic','Ecological Disaster','Climate Change Affecting Economy','Marine Heatwave','High Temperature Pollution','Coral']
    for keyword in keywords:
        params = {
            "search": f"{keyword}",
            "_wrapper_format": "drupal_ajax"
        }
        page = 34
        while True:
            try:
                data = {
                    "view_name": "search_results",
                    "view_display_id": "search",
                    "view_args": "",
                    "view_path": "/views/ajax",
                    "view_base_path": "",
                    "view_dom_id": "ce62540980793c513b5155f21ee9b7c2cbacb9f3dab6e0fd51155edf15337c25",
                    "pager_element": "0",
                    "search": f"{keyword}",
                    "ajax_page_state%5Btheme%5D": "tolonews",
                    # "ajax_page_state%5Blibraries%5D": "classy/base,classy/messages,core/html5shiv,core/normalize,google_analytics/google_analytics,poll/drupal.poll-links,system/base,tolonews/global-styling,tolonews_ajax_views/tolonews_ajax_views,views/views.ajax,views/views.ajax,views/views.ajax,views/views.ajax,views/views.ajax,views/views.ajax,views/views.ajax,views/views.ajax,views/views.ajax,views/views.ajax,views/views.ajax,views/views.ajax,views/views.ajax,views/views.ajax,views/views.ajax,views/views.ajax,views/views.ajax,views/views.ajax,views/views.ajax,views/views.module,views/views.module,views/views.module,views/views.module,views/views.module,views/views.module,views/views.module,views/views.module,views/views.module,views/views.module,views/views.module,views/views.module,views/views.module,views/views.module,views/views.module,views/views.module,views/views.module,views/views.module,views/views.module",
                    "_wrapper_format": "drupal_ajax",
                    "page": f"{page}",
                    "_drupal_ajax": "1",
                    "ajax_page_state[theme_token]": ""
                }
                response = requests.post(url, headers=headers, cookies=cookies, params=params, data=data,impersonate="chrome110").json()[-1].get('data')
                html = etree.HTML(response)
                links = html.xpath("//h3[@class='title-article']/a/@href")
                print(f"当前是关键字{keyword}的第{page}页正在抓取中......")
                if not links:
                    break
                for link in links:
                    # print(links)
                    resp = requests.get("https://tolonews.com"+link, headers=headers, cookies=cookies, impersonate="chrome110").text
                    html2 = etree.HTML(resp)
                    title = html2.xpath("//title/text()")[0]
                    content = "".join(html2.xpath("//div[@class='views-row']//p/text()"))
                    author = ''
                    article_url = "https://tolonews.com"+link
                    pubtime = ''
                    insert_data(title, author, keyword, content, article_url, pubtime)
                page += 1
                time.sleep(random.randint(5, 8))
            except Exception as e:
                print(e)
                continue
        continue


if __name__ == '__main__':
    get_url()
