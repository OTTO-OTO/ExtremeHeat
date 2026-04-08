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
        INSERT INTO Kenya_nation (title, author, keyword, content, article_url, pubtime,country)
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
        "referer": "https://nation.africa/service/search/kenya/290754?pageNum=1&query=rainstorm&sortByDate=true",
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
        "_gcl_au": "1.1.1075009455.1737526089",
        "_gid": "GA1.2.1621508854.1737526089",
        "_sotmsid": "0:m67i67mq:x_1Z707~XhSdi1~APr3uybXVos5XDZc5",
        "_sotmpid": "0:m67i67mq:aINvBxZg1ZS3OEFrzwHgnweyDuAOfba4",
        "_clck": "lvwy2b%7C2%7Cfss%7C0%7C1848",
        "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
        "panoramaId": "c2ba4404dd1662779c299379ee5816d53938f08b6896985ba839d86735aa250d",
        "panoramaIdType": "panoIndiv",
        "ke10004-_zldp": "fDMF1VQmju8zsTiBgAH6kVlTcs%2BiSMiAKYwsYol1lQDwa8qEk6GNBanZ9JebXpLrb8YSslKh1II%3D",
        "ke10004-_zldt": "d2669fa2-80aa-4067-8ce0-23b86a085a51-2",
        "g_state": "{\"i_p\":1737533330017,\"i_l\":1}",
        "sessionId": "9122fdef-79d4-4664-88ec-6247caf58894",
        "FCNEC": "%5B%5B%22AKsRol-6VmviP6loa5JYwYLcXpATVv0zzjIJQjqEJ0xpgyE_ZTEwM5p5arpe_zv8X0IwcYeQQljA01-TU5X4bQDYEHI7fa0Qg7bkVS0WDYpi-ZBy5hpxbjIjiuuOl_a9qz5HTYPxvob1DjdYMsJUxUiBLOXOpo9Z5g%3D%3D%22%5D%5D",
        "cf_clearance": "8LyLydym6w_5bdyE1xn__Q7fiJkCYB5KJr3T_1qUtGw-1737526330-1.2.1.1-wFQ8ws9LOvQSigUB85734nPX77fZp.ytIdSk97Rjiv2szlNQsmlq4RK4XC4qnZ8VqqycOuMxY8dt6KOZF6a2kuYCbB7EKSoeYX8c3PtbWtpklkrWY3TVv16ch3N8cWtH5pLSU8sPZQwXVQgtz29Yorc9uKXLutglEXGZUFdxEXA.hXl1_DnsI00pA9QBbq2jyX0lIwJhyzknCl5.VDQ8HszqvyM_hfPkoi73K4ZOIbRajMPCAynuJpKZI22jYjccaylLXcB9ok2s6X2LBtEGubPkokjlnkbNXW4VfEFJevU",
        "_ga_14SLDP4GRH": "GS1.1.1737526088.1.1.1737526541.60.0.360876959",
        "_ga": "GA1.2.887438154.1737526088",
        "panoramaId_expiry": "1738131327410",
        "_ain_cid": "1737526543044.427430430.61470765",
        "_ain_uid": "1737526543044.506079583.02975154",
        "_clsk": "1hnipti%7C1737526543599%7C9%7C1%7Ck.clarity.ms%2Fcollect",
        "cto_bundle": "Apcqe19PT1JQUkJIcFlMNFJhcloxd0pnNGdmZklPMVBVdzdFSXVyRlhhWmhndTlZeU84VzQ4Y1hDRHJtVCUyRkdKT2xLa2toZWwlMkJzVUdHWHVUN2ZSN0VYQVFuNGQ5JTJCOUpEN1g1MG9rTnNSdGcxYzFGTElUaTNBUXVaMUpkNFVjdzN6MWY4T2J2RUhnVHFpQUVaVlY3UmJhUFElMkZMUSUzRCUzRA"
    }
    keywords =  ["heatwave","Extreme heatwave", "High temperature", "Extreme temperature", "Heatwave event", "Increase in high temperature", "High temperature impact", "Strong heat", "Temperature rise", "Heat event", "Temperature increase", "Heavy rainfall", "Heavy precipitation", "Torrential rain", "Extreme rainfall", "Drought", "Severe drought", "Long-term drought", "Water shortage", "Power outage", "High temperature power outage", "Heatwave power outage", "Power outage caused by high temperature", "Fire", "High temperature fire", "Heat fire", "Temperature fire", "Heat-induced fire", "Agricultural impact", "Heatwave agriculture", "Crop damage", "Agricultural heat stress", "Hypoxia", "Heatstroke", "Heat-induced heatstroke", "High temperature hypoxia", "High temperature heatstroke", "Traffic impact", "High temperature traffic", "Heatwave traffic", "Temperature traffic", "Ecological disaster", "Heat disaster", "High temperature environment", "Impact of heat on biodiversity", "Heatwave ecology", "Pollution", "High temperature pollution", "Heat pollution", "Temperature pollution", "Coral bleaching", "High temperature coral reefs", "Temperature coral bleaching" ]
    for keyword in keywords:
        page = 1
        previous_links = []
        while True:
            try:
                url = "https://nation.africa/service/search/kenya/290754"
                params = {
                    "pageNum": f"{page}",
                    "query": f"{keyword}",
                    "sortByDate": "true"
                }
                response = requests.get(url, headers=headers,cookies=cookies, params=params, impersonate="chrome110").text
                html = etree.HTML(response)
                links = html.xpath("//li[@class='search-result']/a/@href")
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
                    resp = requests.get("https://nation.africa" + link, headers=headers, cookies=cookies,impersonate="chrome110").text
                    # print(resp)
                    html2 = etree.HTML(resp)
                    title = html2.xpath("//h1/text()")[0]
                    content = "".join(html2.xpath("//div[@class='paragraph-wrapper ']//p/text()"))
                    author = ''
                    article_url ="https://nation.africa"+ link
                    pubtime = html2.xpath("//time/@datetime")[0]
                    country = 'Dominica'
                    print(article_url)
                    print("title:", title)
                    print(pubtime)
                    # print("content:", content)
                    # insert_data(title, author, keyword, content, article_url, pubtime, country)

                page += 1
                time.sleep(random.randint(3, 5))
            except Exception as e:
                print(f"抓取过程中发生错误：{e}")
                continue
        continue


if __name__ == '__main__':
    get_url()
