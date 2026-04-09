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
        INSERT INTO South_Africa (title, author, keyword, content, article_url, pubtime,country)
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
        "sec-ch-ua-platform": "\"Windows\"",
        "Referer": "https://www.news24.com/news24/search?query=heatwave",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
        "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
        "HX-Current-URL": "https://www.news24.com/news24/search?query=heatwave",
        "sec-ch-ua-mobile": "?0",
        "HX-Request": "true"
    }

    keywords = ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane",
                "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide",
                "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster",
                "Mudslide", "Landslide"]

    processed_links = set()  # 存储已处理的链接

    for keyword in keywords:
        page = 1
        keyword_processing = True

        while keyword_processing:
            try:
                print(f"当前是关键字 {keyword} 的第 {page} 页正在抓取中......")
                params = {
                    "query": keyword,
                    "pagenumber": page
                }
                response = requests.get("https://www.news24.com/news24/search",
                                        headers=headers,
                                        params=params,
                                        impersonate="chrome110").text
                html = etree.HTML(response)
                links = html.xpath("//article/div/a/@href")

                # 如果当前页没有链接，终止当前关键词处理
                if not links:
                    print(f"关键词 {keyword} 第 {page} 页没有数据，终止处理")
                    keyword_processing = False
                    break

                # 处理当前页链接
                for link in links:
                    full_url = "https://www.news24.com" + link

                    # 检查链接是否已处理过
                    if full_url in processed_links:
                        print(f"链接 {full_url} 已处理过，跳过")
                        continue

                    processed_links.add(full_url)

                    try:
                        resp = requests.get(full_url, headers=headers, impersonate="chrome110").text
                        html2 = etree.HTML(resp)

                        # 提取数据（添加错误处理）
                        title = html2.xpath("//title/text()")[0] if html2.xpath("//title/text()") else "无标题"
                        content = "".join(html2.xpath("//div[@class='article__body NewsArticle']//p/text()"))
                        pubtime = html2.xpath("//meta[@name='publisheddate']/@content")[0] if html2.xpath(
                            "//meta[@name='publisheddate']/@content") else "未知时间"

                        # 插入数据并检查结果
                        if not insert_data(title, "", keyword, content, full_url, pubtime, "South_Africa"):
                            print("检测到重复数据或插入失败，终止当前关键词处理")
                            keyword_processing = False
                            break  # 退出当前链接循环

                    except Exception as e:
                        print(f"处理链接 {full_url} 时出错：{e}")
                        continue

                # 正常翻页
                page += 1
                time.sleep(random.randint(3, 5))

            except Exception as e:
                print(f"请求异常：{e}")
                time.sleep(10)
                continue


if __name__ == '__main__':
    get_url()