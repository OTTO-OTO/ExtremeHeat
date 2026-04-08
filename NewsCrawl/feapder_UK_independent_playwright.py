import feapder
import asyncio
from playwright.async_api import async_playwright
import re
import time
import random
from NewsItems import SpiderDataItem

class IndependentCrawler(feapder.AirSpider):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 原始关键词列表
        original_keywords = ["Extreme", "Heat", "High Temperature", "Heavy Rain", "Drought", "Power Outage from Heat", "Fire", "Air Pollution", "Climate Change", "Crop Yield Reduction", "Oxygen Deficiency", "heat stroke", "High Temperature Affecting Traffic", "Ecological Disaster", "Climate Change Affecting Economy", "Marine Heatwave", "High Temperature Pollution", "Coral"]
        # 将复合关键词拆分为单个单词
        self.keywords = []
        for keyword in original_keywords:
            # 拆分关键词并添加到列表
            words = keyword.split()
            for word in words:
                # 去除特殊字符并转为小写
                clean_word = ''.join(e for e in word if e.isalnum()).lower()
                if clean_word and clean_word not in self.keywords:
                    self.keywords.append(clean_word)
        print(f"拆分后的关键词列表: {self.keywords}")
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.all_articles = []

    def start_requests(self):
        # 运行异步爬虫
        try:
            asyncio.run(self.run_crawler())
        except Exception as e:
            print(f"运行爬虫时出错: {e}")
        
        # 返回空列表，因为我们已经在run_crawler中处理了所有数据
        return []

    async def start_browser(self):
        """启动Playwright浏览器"""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=False,  # 开发阶段设为False以便观察
            args=[
                "--disable-blink-features=AutomationControlled",
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            ]
        )
        self.context = await self.browser.new_context()
        self.page = await self.context.new_page()
        
        # 随机延迟，模拟人类行为
        await asyncio.sleep(random.uniform(1, 3))

    async def close_browser(self):
        """关闭Playwright浏览器"""
        if self.page:
            await self.page.close()
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def search_keyword(self, keyword):
        """搜索关键词并提取结果"""
        # 导航到首页
        await self.page.goto("https://www.independent.co.uk/?CMP=ILC-refresh", wait_until="networkidle")
        await asyncio.sleep(random.uniform(2, 4))
        
        # 输入关键词
        search_box = await self.page.query_selector("#gsc-i-id1")
        if search_box:
            await search_box.click()
            await search_box.fill(keyword)
            await asyncio.sleep(random.uniform(0.5, 1.5))
            await self.page.press("#gsc-i-id1", "Enter")
            await self.page.wait_for_load_state("networkidle")
            await asyncio.sleep(random.uniform(2, 4))
        
        # 提取搜索结果
        all_links = []
        current_page = 1
        previous_links = []
        
        while True:
            print(f"正在处理关键词 '{keyword}' 的第 {current_page} 页")
            
            # 提取当前页的链接
            links = await self.page.evaluate('''
                () => {
                    const results = [];
                    const searchResults = document.querySelectorAll('.gs-title');
                    searchResults.forEach(result => {
                        // 尝试从data-ctorig属性获取链接
                        let url = result.getAttribute('data-ctorig');
                        // 如果没有data-ctorig属性，尝试从href属性获取
                        if (!url) {
                            url = result.getAttribute('href');
                        }
                        if (url && url.includes('independent.co.uk')) {
                            // 清理URL，去除可能的跟踪参数
                            if (url.includes('?')) {
                                url = url.split('?')[0];
                            }
                            // 清理URL中的空格和反引号
                            url = url.trim().replace(/[`]/g, '').replace(/[`]/g, '').replace(/[`]/g, ''); // 三重清理确保反引号被完全移除
                            // 确保URL格式正确
                            if (!url.startsWith('http')) {
                                url = 'https://www.independent.co.uk' + url;
                            }
                            results.push(url);
                        }
                    });
                    return results;
                }
            ''')
            
            # 检查是否和上一页内容相同
            if links == previous_links:
                print(f"检测到和上一页内容相同，停止翻页")
                break
            
            # 去重并添加到总链接列表
            new_links_count = 0
            for link in links:
                if link not in all_links:
                    all_links.append(link)
                    new_links_count += 1
            
            print(f"第 {current_page} 页找到 {len(links)} 个链接，新增 {new_links_count} 个链接，累计 {len(all_links)} 个链接")
            
            # 检查是否有下一页
            next_page_selector = f"div.gsc-cursor-page[aria-label='Page {current_page + 1}']"
            next_page = await self.page.query_selector(next_page_selector)
            
            if next_page:
                # 保存当前页链接用于比较
                previous_links = links.copy()
                
                await next_page.click()
                await self.page.wait_for_load_state("networkidle")
                await asyncio.sleep(random.uniform(2, 4))
                current_page += 1
            else:
                print("没有更多页数，停止翻页")
                break
        
        return all_links

    async def extract_article_content(self, url):
        """提取文章内容"""
        try:
            # 清理URL
            url = url.strip().replace('`', '').replace('`', '').replace('`', '')
            print(f"清理后的URL: {url}")
            
            # 访问URL，使用较短的超时时间
            await self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(random.uniform(1, 3))
            
            # 提取标题
            title_element = await self.page.query_selector('h1')
            title = await title_element.text_content() if title_element else ''
            
            # 提取内容
            content_elements = await self.page.query_selector_all('p')
            content = []
            for element in content_elements:
                text = await element.text_content()
                if text.strip():
                    content.append(text.strip())
            content = '\n'.join(content)
            
            # 打印提取结果
            print(f"提取结果 - 标题: {title[:50]}..." if len(title) > 50 else f"提取结果 - 标题: {title}")
            print(f"提取结果 - 内容长度: {len(content)} 字符")
            
            return {
                "url": url,
                "title": title,
                "content": content
            }
        except Exception as e:
            print(f"提取文章内容时出错: {e}")
            # 继续返回空结果，确保爬虫能够继续运行
            return {
                "url": url,
                "title": "",
                "content": ""
            }

    async def run_crawler(self):
        """运行爬虫"""
        try:
            # 启动浏览器
            await self.start_browser()
            
            # 对每个关键词进行搜索
            for keyword in self.keywords:
                print(f"正在搜索关键词: {keyword}")
                try:
                    links = await self.search_keyword(keyword)
                    
                    # 提取每个链接的内容
                    for link in links:
                        try:
                            print(f"正在提取: {link}")
                            article = await self.extract_article_content(link)
                            
                            # 保存结果
                            if article["title"] and article["content"]:
                                print(f"标题: {article['title']}")
                                print(f"内容长度: {len(article['content'])} 字符")
                                print("-" * 50)
                                
                                # 创建数据项并直接保存到数据库
                                item = SpiderDataItem()
                                item.title = article["title"]
                                item.content = article["content"]
                                item.article_url = article["url"]
                                item.keyword = keyword
                                item.country = "UK"
                                item.pubtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                                
                                # 使用feapder的保存方法
                                try:
                                    self.save_item(item)
                                    print("数据已成功保存到数据库")
                                except Exception as db_error:
                                    print(f"保存到数据库时出错: {db_error}")
                        except Exception as e:
                            print(f"处理链接时出错: {e}")
                            # 继续处理下一个链接
                            continue
                except Exception as e:
                    print(f"搜索关键词时出错: {e}")
                    # 继续处理下一个关键词
                    continue
        except Exception as e:
            print(f"爬虫运行时出错: {e}")
        finally:
            # 关闭浏览器
            try:
                await self.close_browser()
            except Exception as e:
                print(f"关闭浏览器时出错: {e}")

    def save_item(self, item):
        """保存数据到数据库"""
        from feapder.db.mysqldb import MysqlDB
        
        # 创建数据库连接
        db = MysqlDB(
            ip="192.168.42.183", port=3306, db="spider_data", user_name="czm", user_pass="root"
        )
        
        # 检查表是否存在，不存在则创建
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
        
        # 插入数据
        try:
            # 转义特殊字符
            title = item.title.replace("'", "''") if item.title else ""
            author = item.author.replace("'", "''") if item.author else ""
            keyword = item.keyword.replace("'", "''") if item.keyword else ""
            content = item.content.replace("'", "''") if item.content else ""
            article_url = item.article_url.replace("'", "''") if item.article_url else ""
            pubtime = item.pubtime.replace("'", "''") if item.pubtime else ""
            country = item.country.replace("'", "''") if item.country else ""
            
            # 使用字符串格式化来构建SQL语句
            sql = f"""
                INSERT IGNORE INTO `spider_data`.`{table}` 
                (`title`, `author`, `keyword`, `content`, `article_url`, `pubtime`, `country`)
                VALUES ('{title}', '{author}', '{keyword}', '{content}', '{article_url}', '{pubtime}', '{country}')
            """
            db.execute(sql)
            print("数据已成功保存到数据库")
        except Exception as e:
            print(f"保存到数据库时出错: {e}")

if __name__ == "__main__":
    # 创建并运行爬虫
    crawler = IndependentCrawler()
    crawler.start()
