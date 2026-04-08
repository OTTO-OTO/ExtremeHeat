# 付费
import json
import re
import time
import uuid
import random
import os

# 禁用环境变量中的代理设置
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
os.environ['HTTP_PROXY'] = ''
os.environ['HTTPS_PROXY'] = ''

import feapder
import requests
from NewsItems import SpiderDataItem
from feapder.db.mysqldb import MysqlDB
from feapder.network.downloader._requests import RequestsDownloader
from feapder.network.response import Response

# 自定义下载器，确保不使用代理
class CustomRequestsDownloader(RequestsDownloader):
    def download(self, request):
        # 强制使用空代理设置
        kwargs = request.requests_kwargs.copy()
        kwargs['proxies'] = {}
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
    table = "Bolivia"
    
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
        # 禁用代理
        HTTP_PROXY=None,
        HTTPS_PROXY=None,
        # 使用RequestsDownloader
        DOWNLOADER="feapder.network.downloader._requests.RequestsDownloader",
    )

    country = 'Bolivia'
    #西班牙语
    keywords = ["Extremo", "Calor", "Alta", "Temperatura", "Lluvia", "Intensa", "Sequía", "Corte", "de", "Energía", "por", "Calor", "Incendio", "Contaminación", "del", "Aire", "Cambio", "Climático", "Reducción", "de", "la", "Producción", "Agrícola", "Deficiencia", "de", "Oxígeno", "Golpe", "de", "Calor", "Calor", "Afectando", "el", "Tráfico", "Desastre", "Ecológico", "Cambio", "Climático", "Afectando", "la", "Economía", "Ola", "de", "Calor", "Marina", "Contaminación", "por", "Alta", "Temperatura", "Coral"]

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

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.atb.com.bo/pagina/1/"
            params = {
                "s": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

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
            "referer": "https://www.atb.com.bo/pagina/2/?s=calor",
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
        
        # 禁用代理，直接连接
        request.proxies = {}
        
        # 直接修改request.requests_kwargs，确保不包含代理设置
        if not hasattr(request, 'requests_kwargs'):
            request.requests_kwargs = {}
        
        # 完全覆盖requests_kwargs，确保没有代理设置
        request.requests_kwargs = {
            'params': request.requests_kwargs.get('params', {}),
            'headers': request.headers,
            'cookies': request.cookies,
            'timeout': 22,
            'stream': True,
            'verify': False,
            'proxies': {},  # 明确设置为空字典
        }
        
        # 直接修改requests库的默认代理设置
        import requests
        requests.Session().proxies = {}
        
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        
        try:
            # 检查响应状态码
            if response.status_code == 403:
                print(f"关键词 {current_keyword} 遇到403错误，退出当前关键字的循环")
                return None
            
            links = response.xpath("//article//h2[@class='entry-title']/a/@href").extract()
            # print(json.loads(links))
            # # 输出匹配的值
            # for match in matches:
            #     print(match)

            # print(links)
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
            # items.title = item.get("headline")
            items.country = self.country
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://www.atb.com.bo/pagina/{current_page}/"
        params = {
            "s": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        
        # 尝试提取标题
        title = response.xpath("//h1").extract_first()
        if title:
            # 清理HTML标签
            title = re.sub(r'<[^>]+>', '', title).strip()
        if not title:
            title = response.xpath("//h2").extract_first()
            if title:
                # 清理HTML标签
                title = re.sub(r'<[^>]+>', '', title).strip()
        items.title = title if title else "No title"
        
        # 尝试提取内容，使用多个选择器
        content_selectors = [
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
            items.country = "Bolivia"
        
        print(items)
        
        # 检查内容长度，只有内容长度足够的新闻才保存
        content_words = items.content.split()
        if items.content and items.title and len(content_words) > 2:
            yield items
        else:
            print(f"内容太短或缺少必要字段，跳过保存: {items.title}")


if __name__ == "__main__":
    AirSpiderDemo().start()
