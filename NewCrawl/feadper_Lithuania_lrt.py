import feapder
from NewsItems import SpiderDataItem
from lxml import etree
import re
import random
import time
import requests
import ssl

# 忽略SSL错误
ssl._create_default_https_context = ssl._create_unverified_context


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[5, 10],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
    )
    country = 'Lithuania'
    table = 'Lithuania_lrt'
    keywords =['Extreme',
               'Heat','High Temperature','Heavy Rain','Drought','Power Outage from Heat','Fire','Air Pollution','Climate Change','Crop Yield Reduction','Oxygen Deficiency','High Temperature Affecting Traffic','Ecological Disaster','Climate Change Affecting Economy','Marine Heatwave','High Temperature Pollution','Coral'
               ]
    
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:125.0) Gecko/20100101 Firefox/125.0"
    ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            # 添加随机延迟
            time.sleep(random.uniform(2, 4))
            url = "https://www.lrt.lt/api/search"
            params = {
                "page": "1",
                "q": f"{keyword}",
                "count": "44",
                "category_id": "19"
            }
            # 随机选择User-Agent
            user_agent = random.choice(self.user_agents)
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword, meta={"user_agent": user_agent})

    def download_midware(self, request):
        user_agent = request.meta.get("user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0")
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.lrt.lt/",
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
            "user-agent": user_agent
        }
        # 禁用SSL验证
        request.verify = False
        # 移除cookies，因为它们可能会过期或导致问题
        # request.cookies = {
        #     "_ga": "GA1.1.921091325.1735976203",
        #     "cf_clearance": "_9jgCtVUqLZ1rriShM5goS45tqEDHzEh6FovR2TnHlQ-1735976194-1.2.1.1-Y7TB2JlH.ZnCDE3HFeCXCEvw2FIBUL8mYudM1LubQrmlQaBJ99ta4tmpeJE4hOMTaXoR3rTETy5Ri9WrHcoauJFTm8HNMXOvbLhpHB2pqwRNqM2_29Lgb2GNgGnY5xWHxk1yem9_68GrgEDULhgMaDxBQJZIse4NmeUzULUTjEfYxtrB2e.7SKTYLILE07PyQOivwZ7IkEr64GLNaud2SbWyLKkzG5SLEnd_fpG0ipp.Y3f1Gb9XqYeCHup2g5qG00PAfNO30AkotIpVwyha53dKvr49hw37iNAtS3TZu3QzDdYhnpQMlOLFycVPWgzlYmq1LvmKYcwYn6LRowMMpppbagx_KcaRvgx_6Kli3RZu0CaDw1q3MTwAu8l1PaXB1h5BX.oyquk_4E29lzvEeQrRQdb7GGqDaMzB4cEBT2h3t98LBdX8gomStpjv6hOClCjpLN1zr9.g81RyGZB7eA",
        #     "__gads": "ID=4b5331221c94aab2:T=1735976195:RT=1735976195:S=ALNI_MZQhFAbRpSKbM0U19_t_S_9LIPD6w",
        #     "__gpi": "UID=00000fd41ff9d6d8:T=1735976195:RT=1735976195:S=ALNI_MbNgDdra31tWH7LHc4m2T7jlkWg0A",
        #     "__eoi": "ID=600c0b88bb3eb286:T=1735976195:RT=1735976195:S=AA-AfjahZ-37pXAIqO3oE_Y5iHzT",
        #     "FCNEC": "%5B%5B%22AKsRol-syUixryBawZCQunSMmHf6eJ3-WMouy7piemBalTTVNoFk9l7IuW6bQNiomz6kmbA1wwA_WRYYrwExIut_7AdWwGP3KWKhkpIc1Z3sog0PnBPy0OJT1NNVUuPNAdwkl9K9r7T-Quk-qZ44tNiyWnBuYiI6Yg%3D%3D%22%5D%5D",
        #     "_ga_HE30Z4PC4X": "GS1.1.1735976202.1.1.1735976258.4.0.0"
        # }
        return request

    def parse_url(self, request, response):
        try:
            # 测试直接使用requests库
            import requests
            current_keyword = request.keyword
            print(f"当前关键词{current_keyword}的页数为:{request.page}")
            
            # 直接使用requests库发送请求
            url = "https://www.lrt.lt/api/search"
            params = {
                "page": f"{request.page}",
                "q": f"{current_keyword}",
                "count": "44",
                "category_id": "19"
            }
            headers = {
                "user-agent": random.choice(self.user_agents),
                "accept": "application/json, text/plain, */*",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                "referer": "https://www.lrt.lt/"
            }
            
            # 尝试使用代理
            proxies = {
                "http": "http://127.0.0.1:7897",
                "https": "http://127.0.0.1:7897"
            }
            
            print(f"Testing direct request to {url} with params {params}")
            response = requests.get(url, params=params, headers=headers, proxies=proxies, verify=False, timeout=30)
            print(f"Response status code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                links = data.get('items', [])
                print(f"Found {len(links)} items")
                
                if not links:
                    print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
                    return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
                
                for item in links:
                    # print(item)
                    items = SpiderDataItem()
                    items.article_url = "https://www.lrt.lt" + item.get("url")
                    items.title = item.get("title")
                    items.country = self.country
                    items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
                    # 随机选择User-Agent
                    user_agent = random.choice(self.user_agents)
                    yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items, meta={"user_agent": user_agent})

                current_page = request.page + 1
                # 添加随机延迟
                time.sleep(random.uniform(2, 4))
                yield feapder.Request(url, params={
                    "page": f"{current_page}",
                    "q": f"{current_keyword}",
                    "count": "44",
                    "category_id": "19"
                }, callback=self.parse_url, page=current_page, keyword=current_keyword, meta={"user_agent": random.choice(self.user_agents)})
            else:
                print(f"Request failed with status code: {response.status_code}")
                print(f"Response text: {response.text[:500]}")
        except Exception as e:
            print(f"Error parsing URL for keyword {request.keyword}: {e}")
            import traceback
            traceback.print_exc()

    def parse_detail(self, request, response):
        try:
            items = request.items
            items.table_name = self.table
            
            # 直接使用requests库获取新闻详情
            import requests
            headers = {
                "user-agent": random.choice(self.user_agents),
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                "referer": "https://www.lrt.lt/"
            }
            
            # 尝试使用代理
            proxies = {
                "http": "http://127.0.0.1:7897",
                "https": "http://127.0.0.1:7897"
            }
            
            print(f"Fetching detail for URL: {request.url}")
            detail_response = requests.get(request.url, headers=headers, proxies=proxies, verify=False, timeout=30)
            print(f"Detail response status code: {detail_response.status_code}")
            
            if detail_response.status_code == 200:
                from lxml import etree
                html = etree.HTML(detail_response.text)
                items.title = html.xpath("//title/text()")[0] if html.xpath("//title/text()") else items.title
                items.content = "".join(html.xpath("//div[@class='article-block__content']//p/text()"))
                items.author = ''
                items.pubtime = ''
                print(items)
                if items.content:
                    yield items
            else:
                print(f"Detail request failed with status code: {detail_response.status_code}")
        except Exception as e:
            print(f"Error parsing detail for URL {request.url}: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    AirSpiderDemo().start()
