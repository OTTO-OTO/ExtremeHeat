import json
import re
import time
import uuid

import feapder
from feapder import Item
from feapder.db.mysqldb import MysqlDB


class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.42.97", port=3306, db="spider_data", user_name="czm", user_pass="root"
    )
    # 创建表结构
    table = 'UK'
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

        SPIDER_THREAD_COUNT=10,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[6, 10],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
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
    )

    previous_links = {}
    country = 'United Kingdom of Great Britain and Northern Ireland'
    keywords = ["Extreme", "Heat", "High", "Temperature", "Heavy", "Rain", "Drought", "Power", "Outage", "from", "Fire", "Air", "Pollution", "Climate", "Change", "Crop", "Yield", "Reduction", "Oxygen", "Deficiency", "Affecting", "Traffic", "Ecological", "Disaster", "Economy", "Marine", "Heatwave", "Coral", "stroke"]
    
    # 定义不同网站的配置
    sites = [
        {
            "name": "theguardian",
            "search_url": "https://www.google.co.uk/search",
            "search_params": {"as_sitesearch": "www.theguardian.com"},
            "link_selector": "//a[contains(@href, 'theguardian.com') and not(contains(@href, 'google'))]/@href",
            "content_selector": "//div[@id='maincontent']//p/text()",
            "pubtime_selector": "//meta[@property='article:published_time']/@content"
        },
        {
            "name": "bbc",
            "search_url": "https://www.bbc.com/search",
            "search_params": {},
            "link_selector": "",
            "content_selector": "//article//p/text()",
            "pubtime_selector": "//time/@datetime"
        },
        {
            "name": "independent",
            "search_url": "https://www.independent.co.uk/topic/{}",
            "search_params": {},
            "link_selector": "//a[@class='sc-oq6ovx-0 bWGYNn title']/@href",
            "content_selector": "//div[@class='sc-153d5819-0 gVqCzD']//p/text()",
            "pubtime_selector": "//meta[@property='article:published_time']/@content"
        }
    ]
    
    def start_requests(self):
        for site in self.sites:
            for keyword in self.keywords:
                page = 1
                if site["name"] == "independent":
                    url = site["search_url"].format(keyword.lower())
                else:
                    url = site["search_url"]
                    
                params = site["search_params"].copy()
                if site["name"] == "bbc":
                    params["q"] = keyword
                    params["page"] = str(page)
                elif site["name"] == "theguardian":
                    params["q"] = keyword
                
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
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.google.co.uk/",
            "sec-ch-ua": '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        
        # 为 The Independent 添加 cookies
        if request.site and request.site["name"] == "independent":
            request.cookies = {
                "Locale": "UK",
                "gdpr": "0",
                "subscriber_origin": "others",
                "_pc_subscriber_origin": "others",
                "is_mobile_or_tablet": "false",
                "feat__piano_use_vanity_domain": "true",
                "esi-uuid": "2d52993b-e0ae-49ad-bc47-a100d710688d",
                "esi-permutive-id": "2d52993b-e0ae-49ad-bc47-a100d710688d",
                "_lr_geo_location": "HK",
                "__adblocker": "true",
                "s_ecid": "MCMID%7C71846978351038248398722738034738856955",
                "AMCVS_28280C0F53DB09900A490D45%40AdobeOrg": "1",
                "s_cc": "true",
                "_pctx": "%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAEzIEYOBmAFgAYAHACZBg-gDYArAE4A7NMmCQAXyA",
                "_pcid": "%7B%22browserId%22%3A%22mmyczzfq8vxscf6g%22%7D",
                "__pat": "0",
                "__pvi": "eyJpZCI6InYtbW15Y3p6ZnYzcTNmMnQ4bSIsImRvbWFpbiI6Ii5pbmRlcGVuZGVudC5jby51ayIsInRpbWUiOjE3NzM5NzkxNDkyMTV9",
                "__pnahc": "7",
                "__tbc": "%7Bkpex%7DNTpD2gfY6L01HYC8feeY93buW3a-3cFkbXGJJuDZr2rl1spm7_IkOT1irxUZ06Wd",
                "xbc": "%7Bkpex%7Dpb5A5Uz9SQT-iuiNgQRm76Dqxa92buHWdqCe2EG5CNJKyzMmlWZhqo5VufDkyckAkvv_qFUTpPWpDblHIsAAtEYDrodCKy8-8RBme-UiXFtU9BmXlMjM5VfCBOOXnu1K",
                "s_sq": "indepdev%3D%2526pid%253DLogin%2526pidt%253D1%2526oid%253DfunctionJr%252528%252529%25257B%25257D%2526oidt%253D2%2526ot%253DSUBMIT",
                "AMCV_28280C0F53DB09900A490D45%40AdobeOrg": "1585540135%7CMCIDTS%7C20533%7CMCMID%7C71846978351038248398722738034738856955%7CMCAID%7CNONE%7CMCOPTOUT-1773986433s%7CNONE%7CvVersion%7C4.4.0",
                "auth_refresh": "RF2sWrTftc6j2h",
                "loggedIn": "true",
                "uid": "081114da-8cb4-4643-923c-6342701c561b",
                "fullName": "li",
                "truncatedName": "Li",
                "hashed_email": "39f09ee6be00ac15dd236f590fbc635f08b2b2e838bc3765d9337a74978bc026",
                "birth_year": "2000",
                "esi_auth": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL2lkLnBpYW5vLmlvIiwic3ViIjoiMDgxMTE0ZGEtOGNiNC00NjQzLTkyM2MtNjM0MjcwMWM1NjFiIiwiYXVkIjoiU0V6NUNBT1l5SiIsImxvZ2luX3RpbWVzdGFtcCI6IjE3NzM5NzkxNDUzMjYiLCJnaXZlbl9uYW1lIjoibGkiLCJlbWFpbCI6IjcwNzc4NTcxNEBxcS5jb20iLCJlbWFpbF9jb25maXJtYXRpb25fcmVxdWlyZWQiOmZhbHNlLCJwcmVfY29uZmlybWVkX3VzZXIiOmZhbHNlLCJleHAiOjE3NzQwNjU1NDUsImlhdCI6MTc3Mzk3OTE0NSwianRpIjoiVEkwOHRsY0dOcHRjNmoyaCIsInBhc3N3b3JkVHlwZSI6InBhc3N3b3JkIiwibHMiOiJJRCIsInNjIjowLCJ0c2MiOjIsImtleSI6ImtleTAiLCJwVWlkIjoiMDgxMTE0ZGEtOGNiNC00NjQzLTkyM2MtNjM0MjcwMWM1NjFiIiwic2Vzc2lvbiI6InNlc3NfbGFnR1ZCVkYwd21aR3VZeVRRQ1BSb1lBMEJJZGJVcHpzM0ljajZuY0dOQkFnMjBXcUVmN2RFOGdjYkh5bDFUdlV1cktjQT09IiwicmVnaXN0cmF0aW9uRGF0ZSI6MTc3Mzk3ODk5OSwidXNlclN0YXR1cyI6InJlZ2lzdGVyZWQiLCJpc0FkRnJlZVVzZXIiOmZhbHNlLCJpc0FjdGl2ZVJlY3VycmluZ0RvbmF0b3IiOmZhbHNlLCJhZEZyZWVFeHBpcnlUaW1lc3RhbXAiOm51bGwsInJlY3VycmluZ0RvbmF0b3JFeHBpcnlUaW1lc3RhbXAiOm51bGwsInBlcm11dGl2ZUlkIjpudWxsLCJmYW1pbHlfbmFtZSI6IiJ9.0Btqrpw7BWmr6Ne13npalt7QSvD3weel_F39mHXqCTc",
                "esi_puid": "081114da-8cb4-4643-923c-6342701c561b",
                "esi_registration_date": "1773978999",
                "esi_permutive_id_saved": "j%3Anull",
                "AMZN-Token": ""
            }
        
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        site = request.site
        print(f"当前网站{site['name']}，关键词{current_keyword}的页数为:{request.page}")
        
        links = []
        
        # 处理 BBC 的特殊情况，从 __NEXT_DATA__ 中提取链接
        if site["name"] == "bbc":
            data = response.xpath("//script[@id='__NEXT_DATA__']//text()").extract_first()
            if data:
                # 正则表达式模式
                pattern = r'href":"(/[\w/]+)"'
                links = re.findall(pattern, data)
                # 构建完整的BBC URL
                links = ["https://www.bbc.com" + link for link in links]
        else:
            # 提取新闻链接
            links = response.xpath(site["link_selector"]).extract()
            
            # 处理 The Independent 的相对链接
            if site["name"] == "independent":
                full_links = []
                for link in links:
                    if link.startswith('/'):
                        full_links.append("https://www.independent.co.uk" + link)
                    else:
                        full_links.append(link)
                links = full_links
        
        # 去重
        links = list(set(links))
        print(links)
        
        current_page = request.page
        current_links = links
        
        # 检查当前页的链接是否与前一页相同
        key = f"{site['name']}_{current_keyword}"
        if key in self.previous_links and current_links == self.previous_links[key]:
            print(f"网站 {site['name']} 关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理

        self.previous_links[key] = current_links  # 更新上一页的链接列表

        if not links:
            print(f"网站 {site['name']} 关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
            
        for link in links:
            items = Item()
            items.table_name = self.table
            items.article_url = link
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=link, callback=self.parse_detail, items=items, site=site)

        # 翻页
        current_page = request.page + 1
        if site["name"] == "independent":
            url = site["search_url"].format(current_keyword.lower()) + f"?page={current_page}"
            params = {}
        elif site["name"] == "bbc":
            url = site["search_url"]
            params = {}
            params["q"] = current_keyword
            params["page"] = str(current_page)
        else:  # theguardian
            url = site["search_url"]
            params = site["search_params"].copy()
            params["q"] = current_keyword
            params["start"] = str((current_page - 1) * 10)
        
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
        items.title = response.xpath("//title/text()").extract_first()
        
        # 提取文章内容
        content_elements = response.xpath(site["content_selector"]).extract()
        if not content_elements:
            # 尝试通用选择器
            content_elements = response.xpath("//article//p/text()").extract()
        items.content = "".join(content_elements).strip().replace("\r", '').replace("\n", '')
        
        items.author = ''
        items.pubtime = response.xpath(site["pubtime_selector"]).extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()