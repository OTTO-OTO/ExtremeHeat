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
            database="importance_country_data"
        )
        cursor = db.cursor()

        # 插入数据的SQL语句
        insert_sql = """
        INSERT INTO Ecuador (title, author, keyword, content, article_url, pubtime,country)
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
        "referer": "https://www.lahora.com.ec/",
        "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "script",
        "sec-fetch-mode": "no-cors",
        "sec-fetch-site": "cross-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
    }
    cookies = {
        "__Secure-3PSID": "g.a000rggyq3rN6l5a4RzrqfKR8L6w_4oSKSt2-NpZPRMnn-bzQbIYlvrEO6wyUoA2X7nR4dKaLQACgYKAc8SARcSFQHGX2Mi1LRifzVo7z1Yh0GO1dLi3BoVAUF8yKq57cjKXq68AYkR7eUXDFJW0076",
        "__Secure-3PAPISID": "IMadT4_VMCUqIjYe/AiQnZJDT7r7FsfPRc",
        "__Secure-3PSIDTS": "sidts-CjIBmiPuTdxxH6Xb01X7UHquAO1qi0ll-YJl7ZQicoz5mw2YUcxvxSUKtLhh5N-aooWcuRAA",
        "NID": "520=blscRFb8BYbyg_sKToniKO8dwjb7OYxg4O19QrLqqYhdx72HuBYW4aIUU7VO7d2CZxPr2YMN8BePTMmKC1QLMs8fb2BXzQiNf3hSftQORf958UbiW-8R0uspHlNrr0v-p1K8cqhQGCAkcaPG2wFfFLaUx2_hytgvTs_a2PBzc4Y6vSkdQKFNMtbeZ9QPci89fE9ERhWnIVX7ksztDXzqWH_bpC4jPrd9rM5QAFFq-wPaA7M52tL2SKuh5Hach6jn2KZVynJKGBOEzjgv6coRfSukJkcW4z7My8rQhQjq73zmWeurei7dafh-x6zqYFSYM8oBsYKoXnLold5k0LKaYGfpEKD9-oXFIqOq0Y66qjUy7N0hG8IRyAWTNVrxx1e7_l31Vt7ss1LMOWAf0xgfXdRJ8ZnsesoLubpNnMLYHdyl65WYspNolRpD9qAWbSkr-jS3bcPk6qlMy8Hspohlbs6qEUzv5kZa8YYpcZDkU9CDUnccGhADcrniN5oYUndjE6I7xblGyLZKmRFfapzIZnr84ca39K7ak3sn5Ws5xi4V3AHFNZ4STgCeTnjp7N5RAzIXREyQGUmJRe0uwOcgCqxGwsmXruJHgbm3T1H1l03Lk2TW5qcgxBOF_Y7IbyiJHXmgGzT-9T-L_t1pFO3IAwLwAJNEYwrG2G9Fb4_EWLt9ZIsLsSFZYcYbMgvvfZL0ycyhr7iRoXnQ-L6ULl33IXzKqc0hQJ15yPOvB04smxlQ4nX0o1ZFG_KIfBI5UGsNSWuFmHaYU8NfaanJSC41ZG1T9L2nqRlUIk_1lcahQqu4yJLNy7Rml5sFf-Vd4nIJVDOo2GYiM4Oa_nJo",
        "__Secure-3PSIDCC": "AKEyXzVzq6iCbQRofXwPrP7e_oY2jL4Io7x4zFpS8mMR8KBEtu26XeaTUSVtOUljVtRg8QkFaw4"
    }

    keywords = ["calor" ,
               "Ola de calor extrema", "Temperatura alta", "Temperatura extrema", "Evento de ola de calor", "Aumento de la temperatura alta", "Impacto de la temperatura alta", "Temperatura alta", "Calor intenso", "Aumento de la temperatura", "Evento de calor", "Incremento de la temperatura", "Lluvias intensas", "Precipitación fuerte", "Lluvia torrencial", "Lluvias extremas", "Sequía", "Sequía severa", "Sequía prolongada", "Escasez de agua", "Corte de energía", "Corte de energía por alta temperatura", "Corte de energía por ola de calor", "Corte de energía causado por alta temperatura", "Incendio", "Incendio por temperatura alta", "Incendio por calor", "Incendio provocado por temperatura", "Incendio inducido por calor", "Impacto agrícola", "Ola de calor en la agricultura", "Daño a los cultivos", "Estrés térmico agrícola", "Hipoxia", "Golpe de calor", "Golpe de calor inducido por calor", "Hipoxia por temperatura alta", "Golpe de calor por alta temperatura", "Impacto en el tráfico", "Tráfico por alta temperatura", "Tráfico por ola de calor", "Tráfico por temperatura", "Desastre ecológico", "Desastre por calor", "Entorno de temperatura alta", "Impacto del calor en la biodiversidad", "Ecología de la ola de calor", "Contaminación", "Contaminación por alta temperatura", "Contaminación por calor", "Contaminación por temperatura", "Blanqueamiento de corales", "Arrecifes de coral por alta temperatura", "Blanqueamiento de corales por temperatura"
               ]
    for keyword in keywords:
        page = 0
        while True:
            try:
                url = "https://cse.google.com/cse/element/v1"
                params = {
                    "rsz": "filtered_cse",
                    "num": "10",
                    "hl": "en",
                    "start":f"{page}",
                    "source": "gcsc",
                    "cselibv": "5c8d58cbdc1332a7",
                    "cx": "7c6f8afc29e71317c",
                    "q": f"{keyword}",
                    "safe": "off",
                    "cse_tok": "AB-tC_7U5Mva6QzkBd2OdHZ6HfuO:1737007937450",
                    "lr": "",
                    "cr": "",
                    "gl": "",
                    "filter": "0",
                    "sort": "",
                    "as_oq": "",
                    "as_sitesearch": "",
                    "exp": "cc",
                    "fexp": "72821495,72821494",
                    "oq": "calor",
                    "gs_l": "partner-generic.12...0.0.1.1149002.0.0.0.0.0.0.0.0..0.0.csems,nrl=10...0.....34.partner-generic..0.1.165.D2hacmzv2ws",
                    # "g-recaptcha-response": "03AFcWeA4-gg86GYmLE0flsnptTdmOun6If2mxH-qKx5c6scfaUdgLxwo7DMYU949nAf9fpsztON-huhK_awf7lHgGwlAsJ5JuP79IM75Mw_rV_OSz52yZO6KyZ00ZdyigD_ONzpNn1LnpfEfMKKzmg_4_jQRXTEZYK17tZRBmMsLl5d_ygALRmDaxzwMIPWayUWdrgdDk-DTYLEowXWGDzzzkYI_JVf-NBIZhMUhoDltIFFlxvYckYdDba0mskF5g260cjujiomCx7PelhO3IcTeCnpr0SZhRrFIL2PBklVKB5n1OHUXT2zQeltI7iu47S06IkGHaTKPMvf4f1EZzq6a7vecKJ7HeWNBkGh76CB78p1qx3Hu1VwMGTmHrJXrS1P6xeKuKxklBQi-z10iL2mcwPRgkt983263jBMyyZV1hAGW118GScd_7s4io8cgJxKZg84GE0MiUDj5386BRyKC0B28suISyKblgxA9MkDqmok6wyJIz_uoH9Icx8xtygI_UorCN8rP1LA_5l7fsc_VMS81Woh_In5YWuOCffXJK7FLp_Iz3ix9ckFvpX62os2ehrcs_hE07BXcqbtOlTMMdCm9_7Y0CYnVKiZGQg1h7h4sDNYpKzX1KRrFLcIWu795sBh4vss6pfS3Md2FbpOvSo9umvxBdmkSplqjLIVjot1y6_7nG12fQf_6X4FN0575lfYjDJrA5L5TCNJXVATkfKGgYJQvdqn6heePc6m-O6aOBv-R4X1UKALHMto3cnT6mxxglSinkKd5swYi0W1OEJsksX8dTwzFkaT4YKII56nPfH4eCyOBtWf3iKu5GvsArpYA4juU9a3ivLdK3SOMqo-iYXHc8zEMYy0XhX3zovZAhlFKbnpY",
                    "callback": "google.search.cse.api3941",
                    "rurl": "https://www.lahora.com.ec/buscar/?ref=menu"
                }
                response = requests.get(url, headers=headers, params=params, cookies=cookies,
                                        impersonate="chrome110").text
                print(response)
                links = json.loads(response.split('api3941(')[-1].split(");")[0])['results']
                print(links)
                print(f"当前是关键字{keyword}的第{page}页正在抓取中......")
                if not links:
                    break
                for item in links:
                    resp = requests.get(item.get("url"), headers=headers, cookies=cookies,
                                        impersonate="chrome110").text
                    # print(resp)
                    html2 = etree.HTML(resp)
                    title = html2.xpath("//title/text()")[0]
                    content = "".join(html2.xpath("//div[@class='entry-content']//p//text()"))
                    author = ''
                    article_url = item.get("url")
                    pubtime = ''
                    country = 'Ecuador'
                    print(article_url)
                    print("title:", title)
                    print("content:", content)
                    insert_data(title, author, keyword, content, article_url, pubtime, country)
                    if not insert_data(title, author, keyword, content, article_url, pubtime, country):
                        print(f"数据插入失败，退出当前关键词 {keyword} 的循环")
                        break  # 退出当前for循环

                page = (page +1) * 10
                time.sleep(random.randint(3, 5))
            except Exception as e:
                print(e)
                continue
        continue


if __name__ == '__main__':
    get_url()
