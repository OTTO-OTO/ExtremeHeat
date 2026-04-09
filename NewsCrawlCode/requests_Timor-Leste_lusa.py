import random
import time
from curl_cffi import requests
import pymysql
from lxml import etree
from pymysql import IntegrityError


def insert_data(title, author, keyword, content, article_url, pubtime, country):
    try:
        db = pymysql.connect(
            host="192.168.101.200",
            port=3307,
            user="czm",
            password="root",
            database="TropicalCyclone"
        )
        cursor = db.cursor()

        insert_sql = """
        INSERT INTO Timor_Leste (title, author, keyword, content, article_url, pubtime, country)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        data = (title, author, keyword, content, article_url, pubtime, country)
        cursor.execute(insert_sql, data)
        db.commit()
        print("数据插入成功")
        return True
    except IntegrityError as e:
        print(f"重复数据跳过：{e}")
        db.rollback()
        return False
    except Exception as e:
        print(f"数据插入失败：{e}")
        db.rollback()
        return False
    finally:
        cursor.close()
        db.close()


def get_url():
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "priority": "u=0, i",
        "referer": "https://www.lusa.pt/",
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
        # 保留原始cookie配置
        "SessionId": "yuir3yydxxmabhfoczeokyw2",
        "CookieConsent": "{stamp:%27m1SYB+1TAdFcqHkv9x4XWt6jDHOzul7vKBqx0xvp+DQl27UPlI9DkA==%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2Cmethod:%27explicit%27%2Cver:1%2Cutc:1736492462049%2Cregion:%27sg%27}",
        "_ga": "GA1.1.499129418.1736492477",
        "cf_clearance": "IMLKre5Jb0vBxCM9rDzuMH0vdkFLHz.L_RqQ3eCHUd8-1736492467-1.2.1.1-clkFG1NwojZnjBX5MaNGwBNNPeQedX4C5iTY4hHSngu5O6AZ339X51aFQh70vBFIyErN2721qFR8z.xkqd.tubIGCV6FsgMONjqVHtIF1.ySS1Zy69PxxlnceJ4TwL4bG3eMXrG2RFiu3vFgUwV7lxcXocUaLwZ2Jg_ejp8oJNSzI98lq40TsOgFfzYdbfVmi9NK6sbdgDONs75lmlwXUw8SEpx_jFTgw3qgC5kg0EWQZ3pZe.lB7wWJCUjLXSD2w9H6N.FaT7bMUmek7zlSD87mUO.60RALXVAVaWKoNxZ3CTyttEzQq6VYmWN0BsUZ8cwjqQX.5cQHOO78Q8XcYswi4LOk.3CthxV1ivXfiaQra2VLfycIA9FthqfeNtCBOjKPZh0alig_JjlUqjsDhQ",
        "searchopts": "1",
        "jwplayer.captionLabel": "Off",
        "Search_History": "heavy%20rain%2C",
        "_ga_3D8CCPBJQ2": "GS1.1.1736492477.1.1.1736492499.38.0.0"
    }

    keywords = [
        "Tropical cyclone", "Tropical depression", "Tropical storm",
        "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain",
        "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster",
        "Marine disaster", "Strong winds", "Typhoon disaster",
        "Mudslide", "Landslide"
    ]

    processed_urls = set()

    for keyword in keywords:
        page = 1
        duplicate_count = 0
        stop_processing = False
        previous_links = []

        while not stop_processing:
            try:
                url = "https://www.lusa.pt/search-results"
                params = {
                    "kw": keyword,
                    "pg": page,
                    "sort": "release_date desc"
                }

                response = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    cookies=cookies,
                    impersonate="chrome110"
                ).text

                html = etree.HTML(response)
                links = html.xpath("//h3/a/@href")
                print(f"当前是关键字 {keyword} 的第 {page} 页，找到 {len(links)} 个链接")

                # 终止条件1：没有新数据
                if not links:
                    print(f"关键词 {keyword} 的所有页面已抓取完毕")
                    break

                # 终止条件2：连续两页出现相同结果
                if links == previous_links:
                    print(f"连续两页出现相同结果，终止抓取关键词 {keyword}")
                    break

                # 处理当前页数据
                for link in links:
                    if link in processed_urls:
                        print(f"发现重复链接：{link}")
                        duplicate_count += 1
                        if duplicate_count >= 3:
                            print("连续出现多个重复链接，终止当前关键词处理")
                            stop_processing = True
                            break
                        continue

                    processed_urls.add(link)
                    duplicate_count = 0

                    try:
                        resp = requests.get(
                            link,
                            headers=headers,
                            cookies=cookies,
                            impersonate="chrome110"
                        ).text

                        html2 = etree.HTML(resp)
                        title = html2.xpath("//title/text()")[0].strip()
                        content = "".join(html2.xpath("//p[@class='text-paragraph']//text()")).strip()
                        pubtime = html2.xpath("//time/@datetime")[0] if html2.xpath("//time/@datetime") else ""

                        success = insert_data(
                            title=title,
                            author="",
                            keyword=keyword,
                            content=content,
                            article_url=link,
                            pubtime=pubtime,
                            country="Timor-Leste"
                        )

                        if not success:
                            print("检测到重复数据或插入失败，终止当前关键词处理")
                            stop_processing = True
                            break

                    except Exception as e:
                        print(f"处理链接 {link} 时发生错误：{e}")
                        continue

                previous_links = links.copy()
                page += 1
                time.sleep(random.randint(5, 8))

            except Exception as e:
                print(f"抓取过程中发生错误：{e}")
                time.sleep(10)
                continue

        print(f"已完成关键词 {keyword} 的抓取\n")


if __name__ == '__main__':
    get_url()
