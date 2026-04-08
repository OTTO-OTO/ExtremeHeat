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
            database="spider_data"
        )
        cursor = db.cursor()

        # 插入数据的SQL语句
        insert_sql = """
        INSERT INTO Guatemala_lahora (title, author, keyword, content, article_url, pubtime,country)
        VALUES (%s, %s, %s, %s, %s, %s,%s)
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
        "referer": "https://lahora.gt/?s=Extremo",
        "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
        "sec-ch-ua-arch": "\"x86\"",
        "sec-ch-ua-bitness": "\"64\"",
        "sec-ch-ua-full-version": "\"131.0.2903.112\"",
        "sec-ch-ua-full-version-list": "\"Microsoft Edge\";v=\"131.0.2903.112\", \"Chromium\";v=\"131.0.6778.205\", \"Not_A Brand\";v=\"24.0.0.0\"",
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
        "_cb": "0WUgzDmzyLlB84r1A",
        "_fbp": "fb.1.1735109678225.347027557476947740",
        "_gid": "GA1.2.433805682.1735889651",
        "PHPSESSID": "58b625b77d45d071247c1f98e58f670d",
        "_cb_svref": "external",
        "cf_clearance": "rP_xhzu5jd25KdqfCUfRN5EGb1f3VCYbXVt0.WgN_zU-1735890395-1.2.1.1-iSotnJ7yYekojK8SQT3.hwD8e.pydFP4JKCrb2ce9NYeFO50Kaxr3VgM1IF0Ck.UencqXBxj5dxQlJZ1aA8EMnuq5FxuZfsG1vZ2.KybSOACGiROhnzCaeBY3WcJD_hWiF.EZ8rSjhxxs2xn_ncg3veRCaQiGeDcdzN1499kP__Rcvnzrfm.cQ3ULANbo8OgviLn2RrccBIV5EdsZl.yX4USPcdvQWfQlXIUKGoxdBEZwExCrFmctc.krNtFrAkL3g0BIw3kn.SsYVnyBNVCaU08OF0_0zLgqsCRo2xmWbk6_rbeuGielsMtCNMPrHkO55cvvwmD5I0hw1db3RGRUHhV9.cb3NvQPWXXgtxMduBTojiqvBgXOK6a0bT5nKuii7ewhh3j6IXkffHPFLsR9F6VipWGmAL1xPo1EXFehBA_9sk1N3el.xDN.Cm4ebDO8ThuetV2gsPbiLmrThn87Q",
        "__ybotpvd": "1",
        "_chartbeat2": ".1735109571767.1735890439347.1001010001.DcyEnuC4VyN2CUmvOgCC_QQaBK8k5V.2",
        "_ga_384638451": "GS1.1.1735889650.5.1.1735890439.0.0.0",
        "_ga_Z40RKMXWYR": "GS1.1.1735889650.5.1.1735890439.21.0.0",
        "_ga": "GA1.2.13002325.1735109568",
        "_gat_gtag_UA_39586820_14": "1",
        "__gads": "ID=c5a6315b8ea2f3d6:T=1735109635:RT=1735890431:S=ALNI_MYhOen-1VvKscYQnwBaOyFg6GD1FQ",
        "__gpi": "UID=00000fb761570202:T=1735109635:RT=1735890431:S=ALNI_MZTTgHPAGU6N4WB7fO_IemRdJcx3g",
        "__eoi": "ID=a487680e1d753d01:T=1735109635:RT=1735890431:S=AA-AfjZ1OcWk0-_CfHITyusuY3SY",
        "_tfpvi": "YWMyNjkyZWQtZjQ1OC00ZDM5LTg1N2MtMjU2OTA3ZTEyNjU2Iy03LTY%3D",
        "_chartbeat5": "478|1434|%2F%3Fs%3DExtremo|https%3A%2F%2Flahora.gt%2Fpage%2F3%2F%3Fs%3DExtremo|7JWUUX1NCzC22Zaf_UMkvB53YIh||c|M1L-Bu8tIWD_OhjCC1xU3vBruDzJ|lahora.gt|"
    }

    keywords = [
        'Extremo', 'Calor', 'Alta Temperatura', 'Lluvia Intensa', 'Sequía',
        'Apagón por Calor', 'Incendio', 'Contaminación del Aire', 'Cambio Climático',
        'Reducción de la Cosechas', 'Deficiencia de Oxígeno', 'Alta Temperatura Afectando el Tráfico',
        'Desastre Ecológico', 'Cambio Climático Afectando la Economía', 'Ola de Calor Marina',
        'Contaminación por Alta Temperatura', 'Coral'
    ]
    # keywords = ['Heat','High Temperature','Heavy Rain','Drought','Power Outage from Heat','Fire','Air Pollution','Climate Change','Crop Yield Reduction','Oxygen Deficiency','High Temperature Affecting Traffic','Ecological Disaster','Climate Change Affecting Economy','Marine Heatwave','High Temperature Pollution','Coral']
    for keyword in keywords:
        page = 1
        while True:
            try:
                url = f"https://lahora.gt/page/{page}/"
                params = {
                    "s": f"{keyword}"
                }
                response = requests.post(url, headers=headers, cookies=cookies, params=params,
                                         impersonate="safari15_3").text
                print(response)
                html = etree.HTML(response)
                links = html.xpath("//div[@class='item-details']/h3/a/@href")[:6]
                print(f"当前是关键字{keyword}的第{page}页正在抓取中......")
                if not links:
                    break
                for link in links:
                    print(links)
                    resp = requests.get(link, headers=headers, cookies=cookies, impersonate="safari15_3").text
                    html2 = etree.HTML(resp)
                    title = html2.xpath("//title/text()")[0]
                    content = "".join(html2.xpath("//article//div//p/text()"))
                    author = ''
                    article_url = link
                    country = 'Guatemala'
                    pubtime = ''
                    insert_data(title, author, keyword, content, article_url, pubtime, country)
                page += 1
                time.sleep(random.randint(3, 5))
            except Exception as e:
                print(e)
                continue
        continue


if __name__ == '__main__':
    get_url()
