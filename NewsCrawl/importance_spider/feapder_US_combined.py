import json
import re
import time
import uuid

import feapder
from feapder import Item
from feapder.db.mysqldb import MysqlDB
from lxml import etree


class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.42.97", port=3306, db="spider_data", user_name="czm", user_pass="root"
    )
    # 创建表结构
    table = 'United_States'
    create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS `spider_data`.`{table}`  (
                  `id` int NOT NULL AUTO_INCREMENT,
                  `title` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '标题',
                  `author` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '作者',
                  `keyword` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '关键词',
                  `content` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '内容',
                  `article_url` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '文章网址',
                  `pubtime` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '发布时间',
                  `country` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '国家',
                  `news_source_country` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '新闻来源国家',
                  `place` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '地名',
                  `Longitude_latitude` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '经纬度',
                  `english` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '英文',
                  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '写入时间',
                  PRIMARY KEY (`id`) USING BTREE,
                  UNIQUE INDEX `title_uni`(`keyword` ASC, `article_url` ASC) USING BTREE
                ) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

                    """
    db.execute(create_table_sql)
    print(f"{table}创建成功<=================")
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=3,  # 减少并发线程数，降低被检测的风险
        # 增加请求间隔时间，避免被速率限制
        SPIDER_SLEEP_TIME=[10, 15],
        SPIDER_MAX_RETRY_TIMES=3,  # 增加重试次数
        MYSQL_IP="192.168.42.97",
        MYSQL_PORT=3306,
        MYSQL_DB="spider_data",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        MYSQL_AUTO_CREATE_TABLE=True,
        # 数据库连接池配置
        MYSQL_POOL_SIZE=10,  # 连接池大小
        MYSQL_POOL_PRE_PING=True,  # 连接池预 ping
        MYSQL_POOL_RECYCLE=3600,  # 连接回收时间
        MYSQL_CONNECT_TIMEOUT=10,  # 连接超时时间
        ITEM_FILTER_ENABLE=False,  # 禁用 item 去重，使用数据库唯一索引确保不重复
        # 禁用SSL验证，避免连接问题
        VERIFY_SSL=False,
    )

    previous_links = {}
    previous_tokens = {}
    country = 'United States'
    keywords = ["Extreme", "Heat", "High", "Temperature", "Heavy", "Rain", "Drought", "Power", "Outage", "from", "Fire", "Air", "Pollution", "Climate", "Change", "Crop", "Yield", "Reduction", "Oxygen", "Deficiency", "Affecting", "Traffic", "Ecological", "Disaster", "Economy", "Marine", "Heatwave", "Coral", "stroke"]
    print("待抓取的关键词列===========>", keywords)
    
    # 定义不同网站的配置
    sites = [
        {
            "name": "cnn",
            "search_url": "https://search.prod.di.api.cnn.io/content",
            "method": "GET",
            "content_type": "json",
            "link_selector": "result",
            "content_selector": "//article//p//text()",
            "pubtime_selector": "//meta[@property='article:published_time']/@content"
        },
        {
            "name": "washingtonpost",
            "search_url": "https://www.washingtonpost.com/search/api/search/",
            "method": "POST",
            "content_type": "json",
            "link_selector": "body.items",
            "content_selector": "//article//div//p/text()",
            "pubtime_selector": "//meta[@property='article:published_time']/@content"
        },
        {
            "name": "apnews",
            "search_url": "https://apnews.com/search",
            "method": "GET",
            "content_type": "html",
            "link_selector": "//a[contains(@href, 'apnews.com/article')]/@href",
            "content_selector": "//div[@class='Page-storyBody gtmMainScrollContent']//p/text()",
            "title_selector": "//h1[@class='Page-headline']/text()",
            "pubtime_selector": "//meta[@property='article:published_time']/@content"
        }
    ]
    
    def start_requests(self):
        for site in self.sites:
            for keyword in self.keywords:
                page = 1
                if site["name"] == "cnn":
                    url = site["search_url"]
                    params = {
                        "q": f"{keyword}",
                        "size": "10",
                        "from": "10",
                        "page": "1",
                        "sort": "newest",
                        "request_id": f"stellar-search-{uuid.uuid4()}",
                        "site": "cnn"
                    }
                    yield feapder.Request(
                        url, 
                        params=params, 
                        callback=self.parse_url, 
                        page=page, 
                        keyword=keyword, 
                        site=site, 
                        filter_repeat=True
                    )
                elif site["name"] == "washingtonpost":
                    url = site["search_url"]
                    data = {
                        "searchTerm": f"{keyword}",
                        "filters": {
                            "sortBy": "relevancy",
                            "dateRestrict": "",
                            "start": 0,
                            "author": "",
                            "section": "",
                            "nextPageToken": ""
                        }
                    }
                    data = json.dumps(data, separators=(',', ':'))
                    yield feapder.Request(
                        url, 
                        data=data, 
                        callback=self.parse_url, 
                        page=page, 
                        keyword=keyword, 
                        site=site, 
                        filter_repeat=True
                    )
                elif site["name"] == "apnews":
                    url = site["search_url"]
                    params = {
                        "q": f"{keyword}",
                        "p": "1"
                    }
                    yield feapder.Request(
                        url, 
                        params=params, 
                        callback=self.parse_url, 
                        page=page, 
                        keyword=keyword, 
                        site=site, 
                        filter_repeat=True
                    )

    def download_midware(self, request):
        import random
        # 随机User-Agent列表
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
        ]
        
        site = request.site
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "sec-ch-ua": '"Google Chrome";v="130", "Not=A?Brand";v="24", "Chromium";v="130"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": random.choice(user_agents)
        }
        
        if site["name"] == "cnn":
            request.headers["accept"] = "*/*"
            request.headers["origin"] = "https://edition.cnn.com"
            request.headers["referer"] = "https://edition.cnn.com/"
            request.headers["sec-fetch-dest"] = "empty"
            request.headers["sec-fetch-mode"] = "cors"
            request.headers["sec-fetch-site"] = "cross-site"
        elif site["name"] == "washingtonpost":
            request.headers["content-type"] = "application/json"
            request.headers["origin"] = "https://www.washingtonpost.com"
            request.headers["referer"] = "https://www.washingtonpost.com/search/?query=heatwave"
            request.headers["sec-fetch-dest"] = "empty"
            request.headers["sec-fetch-mode"] = "cors"
            request.headers["sec-fetch-site"] = "same-origin"
            request.cookies = {
                "wp_ak_wab": "0|0|1|1|1|1|1|0|1|20230418",
                "wp_usp": "1---",
                "wp_ak_bt": "1|20200518",
                "wp_ak_bfd": "1|20201222",
                "wp_ak_tos": "1|20211110",
                "wp_ak_sff": "1|20220425",
                "wp_ak_lr": "0|20221020",
                "wp_ak_co": "2|20220505",
                "wp_ak_btap": "1|20211118",
                "_cb": "D1sFf3CZb_v6DqHgME",
                "_ga": "GA1.1.1489517971.1734772547",
                "wp_ak_kywrd_ab": "1",
                "wp_ak_v_mab": "0|0|3|1|20250110",
                "_v__chartbeat3": "CbEHiBBG8kYmDLgYXg",
                "wp_ak_subs": "1|20250113",
                "sec_wapo_login_id": "e0d40fce-91fd-49ac-95c6-efc879b1205b",
                "wapo_display": "2687833212",
                "wapo_groups": "default-onestep-short",
                "wapo_login_id": "e0d40fce-91fd-49ac-95c6-efc879b1205b",
                "wapo_provider": "Washington%20Post",
                "wp_ttrid": '"c9bfbf8d-c0aa-4923-82b0-1e81b18279f8.1"',
                "wp_devicetype": "0",
                "wp_ak_signinv2": "1|20230125",
                "wp_geo": "SG||||INTL"
            }
        elif site["name"] == "apnews":
            request.headers["referer"] = "https://apnews.com/search?q=heavy+rain"
            # 简单的cookies，避免使用过期的cookies
            request.cookies = {
                "_ga": "GA1.1.1234567890.1234567890",
                "_gid": "GA1.2.1234567890.1234567890",
                "OptanonAlertBoxClosed": "2025-01-01T00:00:00.000Z",
                "OptanonConsent": "isGpcEnabled=0&datestamp=Wed+Jan+01+2025+00%3A00%3A00+GMT%2B0000&version=202411.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=12345678-1234-1234-1234-123456789012&interactionCount=1&isAnonUser=0&landingPath=NotLandingPage&groups=1%3A1%2C2%3A1%2C3%3A1%2C4%3A1&intType=1"
            }
        
        # 禁用SSL验证
        request.verify = False
        
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        site = request.site
        print(f"当前网站{site['name']}，关键词{current_keyword}的页数为:{request.page}")
        
        links = []
        next_token = None
        
        if site["name"] == "cnn":
            data = response.json
            links = data['result']
        elif site["name"] == "washingtonpost":
            data = response.json
            links = data['body'].get("items", [])
            next_token = data['body'].get("searchInformation", {}).get("nextPageToken")
        elif site["name"] == "apnews":
            # 尝试多种方式提取链接
            links = []
            
            # 方式1：直接提取包含article的链接
            links.extend(response.xpath(site["link_selector"]).extract())
            
            # 方式2：提取所有a标签，然后筛选包含article的链接
            all_links = response.xpath("//a/@href").extract()
            for link in all_links:
                if "apnews.com/article" in link and link not in links:
                    links.append(link)
            
            # 方式3：提取可能的相对链接并转换为绝对链接
            relative_links = response.xpath("//a[contains(@href, '/article')]/@href").extract()
            for link in relative_links:
                if link.startswith('/'):
                    full_link = "https://apnews.com" + link
                    if full_link not in links:
                        links.append(full_link)
            
            links = list(set(links))
        
        current_page = request.page
        
        # 检查是否有新内容
        key = f"{site['name']}_{current_keyword}"
        if site["name"] == "washingtonpost":
            if key in self.previous_tokens and self.previous_tokens[key] == next_token:
                print(f"网站 {site['name']} 关键词 {current_keyword} 的下一页没有新内容，退出当前关键词的循环")
                return None
            self.previous_tokens[key] = next_token
        else:
            if key in self.previous_links and self.previous_links[key] == links:
                print(f"网站 {site['name']} 关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
                return None
            self.previous_links[key] = links
        
        if not links:
            print(f"网站 {site['name']} 关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None
            
        for item in links:
            if site["name"] == "cnn":
                article_url = item.get("path")
                title = item.get("headline")
            elif site["name"] == "washingtonpost":
                article_url = item.get("link")
                title = item.get("title")
            elif site["name"] == "apnews":
                article_url = item
                title = None
            
            # 跳过视频链接
            if article_url and "video" in article_url:
                continue
            
            items = Item()
            items.table_name = self.table
            items.article_url = article_url
            items.title = title
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=article_url, callback=self.parse_detail, items=items, site=site)

        # 翻页
        current_page = request.page + 1
        if site["name"] == "cnn":
            url = site["search_url"]
            params = {
                "q": f"{current_keyword}",
                "size": "10",
                "from": "10",
                "page": f"{current_page}",
                "sort": "newest",
                "request_id": f"stellar-search-{uuid.uuid4()}",
                "site": "cnn"
            }
            yield feapder.Request(
                url, 
                params=params, 
                callback=self.parse_url, 
                page=current_page, 
                keyword=current_keyword, 
                site=site, 
                filter_repeat=True
            )
        elif site["name"] == "washingtonpost" and next_token:
            url = site["search_url"]
            data = {
                "searchTerm": f"{current_keyword}",
                "filters": {
                    "sortBy": "relevancy",
                    "dateRestrict": "",
                    "start": 0,
                    "author": "",
                    "section": "",
                    "nextPageToken": f"{next_token}"
                }
            }
            data = json.dumps(data, separators=(',', ':'))
            yield feapder.Request(
                url, 
                data=data, 
                callback=self.parse_url, 
                page=current_page, 
                keyword=current_keyword, 
                site=site, 
                filter_repeat=True
            )
        elif site["name"] == "apnews":
            url = site["search_url"]
            params = {
                "q": f"{current_keyword}",
                "p": f"{current_page}"
            }
            yield feapder.Request(
                url, 
                params=params, 
                callback=self.parse_url, 
                page=current_page, 
                keyword=current_keyword, 
                site=site, 
                filter_repeat=True
            )



    def parse_detail(self, request, response):
        items = request.items
        site = request.site
        items.table_name = self.table
        
        # 解析页面内容
        tree = etree.HTML(response.text)
        
        # 尝试从用户提供的class中提取标题
        title = tree.xpath("//h1[@class='Page-headline']/text()")
        title = title[0].strip() if title else None
        
        # 如果没有找到，尝试使用通用选择器
        if not title:
            title = tree.xpath("//h1/text()")
            title = title[0].strip() if title else None
        
        items.title = title
        
        # 尝试从用户提供的class中提取内容
        content_elements = tree.xpath("//div[@class='Page-storyBody gtmMainScrollContent']//p/text()")
        
        # 如果没有找到，尝试使用原来的选择器
        if not content_elements:
            content_elements = tree.xpath("//div[@class='RichTextStoryBody RichTextBody']//p/text()")
        
        # 如果仍然没有找到，尝试通用选择器
        if not content_elements:
            content_elements = tree.xpath("//article//p/text()")
        
        items.content = "".join(content_elements).strip().replace("\r", '').replace("\n", '')
        
        # 提取发布时间
        pubtime = tree.xpath("//meta[@property='article:published_time']/@content")
        items.pubtime = pubtime[0] if pubtime else None
        
        items.author = ''
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()