# 付费
import json
import re
import time
import uuid
import random
import os

# 启用环境变量中的代理设置
# 系统代理已经配置为 http://127.0.0.1:7897

import requests
import feapder
from NewsItems import SpiderDataItem
from feapder.db.mysqldb import MysqlDB
from feapder.network.downloader._requests import RequestsDownloader
from feapder.network.response import Response

# 自定义下载器，确保使用正确的代理设置
class CustomRequestsDownloader(RequestsDownloader):
    def download(self, request):
        # 使用请求中指定的代理设置
        kwargs = request.requests_kwargs.copy()
        # 确保代理设置存在
        if 'proxies' not in kwargs:
            # 默认使用系统代理
            proxy = 'http://127.0.0.1:7897'
            kwargs['proxies'] = {
                'http': proxy,
                'https': proxy
            }
        response = requests.request(
            request.method, request.url, **kwargs
        )
        response = Response(response)
        return response


class AirSpiderDemo(feapder.AirSpider):
    # 数据库连接和表结构创建
    db = MysqlDB(
        ip="192.168.42.183", port=3306, db="spider_data", user_name="czm", user_pass="root"
    )
    mysql_db = "spider_data"
    table = "Brazil"
    
    # 创建表结构
    create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS `{mysql_db}`.`{table}`  (
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

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[8, 10],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.42.183",
        MYSQL_PORT=3306,
        MYSQL_DB="spider_data",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=False,  # 禁用 item 去重，使用数据库唯一索引确保不重复
        # 使用系统默认代理
        # HTTP_PROXY=None,
        # HTTPS_PROXY=None,
        # 使用自定义下载器
        DOWNLOADER="__main__.CustomRequestsDownloader",
    )

    country = 'Brazil'
    table = 'Brazil'
    
    def __init__(self):
        super().__init__()
        self.previous_links = {}  # 每个关键词独立的链接跟踪

    # 随机User-Agent列表
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0"
    ]
    
    # 葡萄牙语
    keywords = ["Extremo", "Calor", "Alta", "Temperatura", "Chuva", "Intensa", "Seca", "Corte", "de", "Energia", "por", "Calor", "Incêndio", "Poluição", "do", "Ar", "Mudança", "Climática", "Redução", "da", "Produção", "Agrícola", "Deficiência", "de", "Oxigênio", "Onda", "de", "Calor", "Calor", "Afetando", "o", "Tráfego", "Desastre", "Ecológico", "Mudança", "Climática", "Afetando", "a", "Economia", "Onda", "de", "Calor", "Marinha", "Poluição", "por", "Alta", "Temperatura", "Coral"]
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://search.folha.uol.com.br/search"
            params = {
                "q": f"{keyword}",
                "site": "todos",
                "periodo": "todos",
                "sr": "1",
                # "results_count": "456",
                "search_time": "0,033",
                # "url": "https://search.folha.uol.com.br/search?q=Temperatura%20extrema&site=todos&periodo=todos&sr=51"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword, filter_repeat=True)

    def download_midware(self, request):
        # 随机选择User-Agent
        user_agent = random.choice(self.user_agents)
        
        # 随机延迟
        time.sleep(random.uniform(1, 3))
        
        # 完全重写请求头
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://search.folha.uol.com.br/search",
            "sec-ch-ua": f"\"Microsoft Edge\";v=\"{random.randint(130, 135)}\", \"Chromium\";v=\"{random.randint(130, 135)}\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": user_agent
        }
        
        # 添加动态cookies
        request.cookies = {
            "_ga": f"GA1.1.{random.randint(100000000, 999999999)}.{int(time.time())}",
            "_ga_1F7DWH93C0": f"GS1.1.{int(time.time())}.1.1.{int(time.time())}.53.0.0"
        }
        
        # 明确指定代理设置
        proxy = 'http://127.0.0.1:7897'
        proxies = {
            'http': proxy,
            'https': proxy
        }
        
        # 打印调试信息
        print(f"Using proxy: {proxy}")
        print(f"Proxy settings: {proxies}")
        
        # 设置请求参数
        if not hasattr(request, 'requests_kwargs'):
            request.requests_kwargs = {}
        
        # 设置请求参数，明确指定代理
        request.requests_kwargs = {
            'params': request.requests_kwargs.get('params', {}),
            'headers': request.headers,
            'cookies': request.cookies,
            'timeout': 22,
            'stream': True,
            'verify': False,
            'proxies': proxies,  # 明确设置代理
        }
        
        # 确保requests库使用指定的代理
        import requests
        session = requests.Session()
        session.proxies = proxies
        
        # 测试代理连接
        try:
            test_response = session.get('https://www.google.com', timeout=5, verify=False)
            print(f"Proxy test successful: {test_response.status_code}")
        except Exception as e:
            print(f"Proxy test failed: {e}")
        
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        
        try:
            # 检查响应状态码
            if response.status_code == 403:
                print(f"关键词 {current_keyword} 遇到403错误，退出当前关键字的循环")
                return None
            
            links = response.xpath("//div[@class='c-headline__content']/a/@href").extract()
            print(links)
            current_page = request.page

            current_links = links
            if current_keyword in self.previous_links and current_links == self.previous_links[current_keyword]:
                print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
                return None  # 如果链接相同，返回 None 表示结束当前关键词的处理

            self.previous_links[current_keyword] = current_links  # 更新当前关键词的链接列表

            if not links:
                print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
                return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        except Exception as e:
            print(f"关键词 {current_keyword} 解析出错: {str(e)}")
            return None
            
        for item in links:
            items = SpiderDataItem()
            items.table_name = self.table
            items.article_url = item
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 25
        url = "https://search.folha.uol.com.br/search"
        params = {
            "q": f"{current_keyword}",
            "site": "todos",
            "periodo": "todos",
            "sr": f"{current_page}",
            # "results_count": "456",
            "search_time": "0,033",
            # "url": f"https://search.folha.uol.com.br/search?q={current_keyword}&site=todos&periodo=todos&sr={}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword, filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        
        # 尝试提取标题
        title = response.xpath("//h1/text()").extract_first()
        if title:
            # 清理HTML标签
            title = re.sub(r'<[^>]+>', '', title).strip()
        if not title:
            title = response.xpath("//title/text()").extract_first()
            if title:
                # 清理HTML标签
                title = re.sub(r'<[^>]+>', '', title).strip()
        items.title = title if title else "No title"
        
        # 尝试提取内容，使用多个选择器
        content_selectors = [
            "//div[@class='article__body']//p/text()",
            "//article//p/text()",
            "//div[@class='content']//p/text()",
            "//main//p/text()",
            "//div[@class='article-content']//p/text()",
            "//p/text()"
        ]
        
        content = ""
        for selector in content_selectors:
            content_parts = response.xpath(selector).extract()
            if content_parts:
                content = " ".join(content_parts)
                break
        
        # 处理特殊字符
        content = content.replace("'", "\\'").replace('"', '\\"')
        items.content = content if content else "No content"
        
        # 尝试提取发布时间
        pubtime = response.xpath("//time/@datetime").extract_first()
        if not pubtime:
            pubtime = response.xpath("//span[@class='publish-time']/text()").extract_first()
        items.pubtime = pubtime if pubtime else "No pubtime"
        
        items.author = ""
        print(f"提取到的标题: {items.title}")
        print(f"提取到的内容长度: {len(items.content)}")
        print(f"提取到的发布时间: {items.pubtime}")
        print(f"提取到的URL: {items.article_url}")
        print(f"提取到的关键词: {items.keyword}")
        
        # 确保所有必要字段都有值
        if not items.title:
            items.title = "No title"
        if not items.content:
            items.content = "No content"
        if not items.article_url:
            items.article_url = "No url"
        if not items.keyword:
            items.keyword = "No keyword"
        if not items.country:
            items.country = "Brazil"
        
        print(items)
        
        # 检查内容长度，只有内容长度足够的新闻才保存
        content_words = items.content.split()
        if items.content and items.title and len(content_words) >= 100:
            yield items
        else:
            print(f"内容太短或缺少必要字段，跳过保存: {items.title}")


if __name__ == "__main__":
    AirSpiderDemo().start()
