# -*- coding: utf-8 -*-

import asyncio
from playwright.async_api import async_playwright
from feapder.db.mysqldb import MysqlDB
import random
import time
import string
import hashlib

class FranceCoralPlaywrightSpider:
    def __init__(self):
        self.db = MysqlDB(
            ip="192.168.42.183", port=3306, db="spider_data", user_name="czm", user_pass="root"
        )
        # 创建表结构
        self.table = 'France_2'
        self.create_table_sql = f"""
                        CREATE TABLE IF NOT EXISTS `spider_data`.`{self.table}`  (
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
        self.db.execute(self.create_table_sql)
        print(f"{self.table}创建成功<=================")

        self.country = 'France'
        # 只测试Coral关键词
        self.keywords = [ 
            "Extreme", "Heat", "High", "Temperature", "Heavy", "Rain", "Drought", "Power", "Outage", "from", "Fire", "Air", "Pollution", "Climate", "Change", "Crop", "Yield", "Reduction", "Oxygen", "Deficiency", "heat", "stroke", "Affecting", "Traffic", "Ecological", "Disaster", "Affecting", "Economy", "Marine", "Heatwave", "Pollution", "Coral", "extrême", "chaleur", "températures", "élevées", "fortes", "pluies", "sécheresse", "panne", "d'électricité", "due", "à", "la", "incendie", "pollution", "de", "l'air", "changement", "climatique", "réduction", "des", "rendements", "agricoles", "hypoxie", "coup", "de", "impact", "sur", "le", "trafic", "désastre", "écologique", "impact", "du", "changement", "climatique", "sur", "l'économie", "vague", "de", "chaleur", "marine", "pollution", "liée", "à", "la", "chaleur", "corail"
        ]
        
        # 随机用户代理列表
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/122.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/131.0.0.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0"
        ]
        
        self.previous_links = {}  # 使用字典存储每个关键词的上一页链接

    def generate_session_id(self):
        """生成随机会话ID"""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=32))

    async def run(self):
        async with async_playwright() as p:
            # 启动浏览器
            browser = await p.chromium.launch(
                headless=True,  # 启用无头模式，提高速度
                slow_mo=0,  # 取消操作延迟
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-gpu",
                    "--disable-extensions",
                    "--disable-dev-shm-usage"
                ]
            )
            
            for keyword in self.keywords:
                await self.process_keyword(browser, keyword)
            
            await browser.close()

    async def process_keyword(self, browser, keyword):
        print(f"开始处理关键词: {keyword}")
        page = await browser.new_page()
        
        # 设置随机用户代理
        user_agent = random.choice(self.user_agents)
        await page.set_extra_http_headers({
            "User-Agent": user_agent
        })
        
        # 访问搜索页面
        import datetime
        end_date = datetime.datetime.now().strftime("%d/%m/%Y")
        url = f"https://www.lemonde.fr/recherche/?search_keywords={keyword}&start_at=19/12/1944&end_at={end_date}&search_sort=relevance_desc&page=1"
        
        print(f"访问搜索页面: {url}")
        await page.goto(url, wait_until="networkidle", timeout=60000)
        
        # 减少延迟，提高速度
        await asyncio.sleep(random.uniform(0.5, 1))
        
        # 处理分页
        current_page = 1
        
        while True:
            print(f"处理关键词 {keyword} 的第 {current_page} 页")
            
            # 提取新闻链接
            links = await self.extract_links(page)
            
            # 检查是否有新链接
            if keyword in self.previous_links and links == self.previous_links[keyword]:
                print(f"关键词 {keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
                break
            
            # 更新当前关键词的上一页链接
            self.previous_links[keyword] = links
            
            # 处理每个链接
            for link in links:
                await self.process_news_page(browser, link, keyword)
            
            # 尝试点击下一页
            has_next_page = await self.go_to_next_page(page)
            if not has_next_page:
                print(f"关键词 {keyword} 没有更多页面了")
                break
            
            current_page += 1
            # 随机延迟，避免请求过于集中
            await asyncio.sleep(random.uniform(3, 6))
        
        await page.close()

    async def extract_links(self, page):
        """提取新闻链接"""
        links = []
        
        # 尝试多种选择器
        selectors = [
            "//section[@class='js-river-search']//a[@class='js-teaser__link teaser__link']",
            "//section[@class='teaser__list teaser__list--block teaser__list--friend teaser__list--search old__teaser-list']//a",
            "//section[@class='js-teaser teaser teaser--inline-picture']//a",
            "//a[@class='js-teaser__link teaser__link']",
            "//a[contains(@class, 'teaser__link')]"
        ]
        
        for selector in selectors:
            elements = await page.query_selector_all(selector)
            for element in elements:
                href = await element.get_attribute("href")
                if href:
                    if not href.startswith('http'):
                        href = "https://www.lemonde.fr" + href
                    links.append(href)
        
        # 去重
        links = list(set(links))
        
        # 过滤掉非新闻链接
        filtered_links = []
        for link in links:
            if link.startswith("https://www.lemonde.fr/") and "/recherche/" not in link and "/tag/" not in link and "/rubrique/" not in link:
                filtered_links.append(link)
        
        print(f"找到 {len(filtered_links)} 个新闻链接")
        return filtered_links

    async def process_news_page(self, browser, url, keyword):
        """处理新闻页面"""
        print(f"处理新闻: {url}")
        
        # 创建新的浏览器上下文
        context = await browser.new_context()
        
        # 设置随机用户代理
        user_agent = random.choice(self.user_agents)
        await context.set_extra_http_headers({
            "User-Agent": user_agent
        })
        
        # 打开新页面
        news_page = await context.new_page()
        
        try:
            # 访问新闻页面
            await news_page.goto(url, wait_until="networkidle", timeout=60000)
            
            # 减少延迟，提高速度
            await asyncio.sleep(random.uniform(0.2, 0.5))
            
            # 提取数据
            title = await self.extract_title(news_page)
            content = await self.extract_content(news_page)
            pubtime = await self.extract_pubtime(news_page)
            author = await self.extract_author(news_page)
            
            # 保存到数据库
            if title and content:
                self.save_to_db(title, author, keyword, content, url, pubtime)
                print(f"成功保存新闻: {title}")
            else:
                print(f"新闻 {url} 数据不完整，跳过")
                
        except Exception as e:
            print(f"处理新闻 {url} 时出错: {str(e)}")
        finally:
            await news_page.close()
            await context.close()
            # 减少延迟，提高速度
            await asyncio.sleep(random.uniform(0.1, 0.3))

    async def extract_title(self, page):
        """提取标题"""
        selectors = [
            "//h1[@class='ds-title']",
            "//h1[@class='article__title']",
            "//h1[@class='article__title article__title--xxl']",
            "//h1",
            "//title"
        ]
        
        for selector in selectors:
            element = await page.query_selector(selector)
            if element:
                text = await element.text_content()
                if text:
                    return text.strip()
        
        return None

    async def extract_content(self, page):
        """提取内容"""
        selectors = [
            "//p[@class='article__paragraph ']",
            "//div[@class='article__content old__article-content-single']",
            "//article[@class='article__content old__article-content-single']",
            "//article",
            "//div[@class='content']"
        ]
        
        content = []
        for selector in selectors:
            elements = await page.query_selector_all(selector)
            for element in elements:
                text = await element.text_content()
                if text:
                    content.append(text)
        
        if content:
            return "".join(content).strip().replace("\r", '').replace("\n", '')
        
        return None

    async def extract_pubtime(self, page):
        """提取发布时间"""
        selectors = [
            "//time",
            "//span[@class='meta__date']",
            "//span[@class='meta__date meta__date--header']",
            "//span[@class='article__date']"
        ]
        
        for selector in selectors:
            element = await page.query_selector(selector)
            if element:
                # 尝试获取datetime属性
                datetime = await element.get_attribute("datetime")
                if datetime:
                    return datetime
                # 否则获取文本内容
                text = await element.text_content()
                if text:
                    return text.strip()
        
        return None

    async def extract_author(self, page):
        """提取作者"""
        selectors = [
            "//span[@class='meta__author meta__author--no-after']",
            "//span[@class='article__author']",
            "//div[@class='article__author']",
            "//span[@class='author']",
            "//span[@class='meta__author meta__author--page']"
        ]
        
        for selector in selectors:
            element = await page.query_selector(selector)
            if element:
                text = await element.text_content()
                if text:
                    return text.strip()
        
        return None

    async def go_to_next_page(self, page):
        """点击下一页"""
        try:
            # 尝试多种下一页选择器
            next_page_selectors = [
                "//a[@rel='next']",
                "//button[contains(text(), 'Suivant')]",
                "//a[contains(text(), 'Suivant')]",
                "//button[contains(text(), 'Next')]",
                "//a[contains(text(), 'Next')]"
            ]
            
            for selector in next_page_selectors:
                next_button = await page.query_selector(selector)
                if next_button:
                    await next_button.click()
                    await page.wait_for_load_state("networkidle", timeout=60000)
                    return True
            
            # 尝试通过页码点击
            current_page_selector = "//span[@class='pagination__item pagination__item--active']"
            current_page_element = await page.query_selector(current_page_selector)
            if current_page_element:
                current_page_text = await current_page_element.text_content()
                try:
                    current_page_num = int(current_page_text)
                    next_page_num = current_page_num + 1
                    next_page_selector = f"//a[contains(@class, 'pagination__item') and text()='{next_page_num}']"
                    next_page_button = await page.query_selector(next_page_selector)
                    if next_page_button:
                        await next_page_button.click()
                        await page.wait_for_load_state("networkidle", timeout=60000)
                        return True
                except ValueError:
                    pass
            
            return False
        except Exception as e:
            print(f"点击下一页时出错: {str(e)}")
            return False

    def save_to_db(self, title, author, keyword, content, article_url, pubtime):
        """保存到数据库"""
        try:
            # 转义单引号，避免SQL语法错误
            title = title.replace("'", "''") if title else ""
            author = author.replace("'", "''") if author else ""
            content = content.replace("'", "''") if content else ""
            article_url = article_url.replace("'", "''") if article_url else ""
            pubtime = pubtime.replace("'", "''") if pubtime else ""
            
            # 构建插入SQL
            insert_sql = f"""
                INSERT IGNORE INTO `spider_data`.`{self.table}` 
                (`title`, `author`, `keyword`, `content`, `article_url`, `pubtime`, `country`)
                VALUES ('{title}', '{author}', '{keyword}', '{content}', '{article_url}', '{pubtime}', '{self.country}')
            """
            
            # 执行插入
            self.db.execute(insert_sql)
        except Exception as e:
            print(f"保存到数据库时出错: {str(e)}")

if __name__ == "__main__":
    spider = FranceCoralPlaywrightSpider()
    asyncio.run(spider.run())
