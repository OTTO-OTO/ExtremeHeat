import feapder
from NewsItems import SpiderDataItem
from lxml import etree
import re
import time
import random

class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(
        SPIDER_THREAD_COUNT=1,  # 爬虫并发数，追求速度推荐32
        SPIDER_SLEEP_TIME=[5, 8],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
    )
    country = 'Malawi'
    table = 'Malawi_nyasatimes'
    
    # 气候变化相关关键词列表
    climate_keywords = [
        # 基础关键词
        'heat', 'temperature', 'rain', 'drought', 'fire', 'pollution',
        'climate', 'disaster', 'weather', 'warm', 'hot', 'cold',
        'flood', 'storm', 'cyclone', 'hurricane', 'drought', 'rainfall',
        'temperature', 'climate change', 'global warming', 'greenhouse',
        'carbon', 'emission', 'environment', 'ecology', 'sustainability',
        'renewable', 'energy', 'fossil fuel', 'deforestation', 'wildfire',
        'air quality', 'water quality', 'biodiversity', 'conservation',
        'extreme weather', 'natural disaster', 'sea level', 'ice melt',
        'ocean acidification', 'coral bleaching', 'heatwave', 'cold wave',
        'drought', 'flooding', 'landslide', 'wildfire', 'air pollution',
        'water pollution', 'soil pollution', 'noise pollution', 'light pollution',
        'climate emergency', 'climate action', 'net zero', 'carbon neutral',
        'renewable energy', 'solar power', 'wind power', 'hydro power',
        'geothermal', 'biomass', 'energy efficiency', 'sustainable development',
        'circular economy', 'eco-friendly', 'green technology', 'clean energy',
        'climate adaptation', 'climate resilience', 'climate mitigation',
        'carbon footprint', 'carbon offset', 'carbon tax', 'emissions trading',
        'renewable portfolio standard', 'feed-in tariff', 'green bonds',
        'sustainable investing', 'ESG', 'environmental justice', 'climate refugees',
        'extreme heat', 'high temperature', 'heavy rain', 'drought',
        'power outage from heat', 'fire', 'air pollution', 'climate change',
        'crop yield reduction', 'oxygen deficiency', 'high temperature affecting traffic',
        'ecological disaster', 'climate change affecting economy', 'marine heatwave',
        'high temperature pollution', 'coral'
    ]

    def start_requests(self):
        # 直接访问 nyasatimes.com 网站
        base_url = "https://www.nyasatimes.com"
        print(f"开始爬取 {base_url}")
        yield feapder.Request(base_url, callback=self.parse_nyasatimes_home, meta={"page": 1})
        
        # 直接搜索气候变化相关关键词
        search_keywords = [
            'climate change', 'global warming', 'drought', 'flood',
            'extreme weather', 'environmental disaster', 'pollution'
        ]
        
        for keyword in search_keywords:
            search_url = f"https://www.nyasatimes.com/?s={keyword.replace(' ', '+')}"
            print(f"搜索关键词: {keyword}")
            yield feapder.Request(search_url, callback=self.parse_search_results, meta={"keyword": keyword, "page": 1})

    def download_midware(self, request):
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:125.0) Gecko/20100101 Firefox/125.0"
        ]
        request.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": random.choice(user_agents)
        }
        return request

    def parse_nyasatimes_home(self, request, response):
        current_page = request.meta.get("page", 1)
        print(f"当前页码: {current_page}")
        
        # 提取新闻链接
        links = response.xpath("//a[@href]/@href").extract()
        
        # 构建完整链接
        clean_links = []
        for link in links:
            if link.startswith('http'):
                clean_links.append(link)
            elif link.startswith('/'):
                clean_links.append(f"https://www.nyasatimes.com{link}")
        
        # 去重
        clean_links = list(set(clean_links))
        
        # 过滤出新闻链接（排除分类页面和其他非新闻链接）
        news_links = []
        category_links = []
        
        for link in clean_links:
            # 排除非新闻链接
            if any(exclude in link for exclude in ['/category/', '/tag/', '/author/', '#', 'javascript:', 'mailto:', 'twitter.com', 'facebook.com', 'instagram.com', 'advertise', 'fact-check', 'about', 'contact']):
                if '/category/news/' in link:
                    category_links.append(link)
                continue
            # 包含新闻相关关键词的链接
            if any(keyword in link for keyword in ['/20', '/news/', '/article/', '-']):
                news_links.append(link)
        
        # 打印所有提取的链接用于调试
        print(f"提取到的所有链接: {len(clean_links)}")
        print(f"找到 {len(news_links)} 个新闻链接")
        print(f"找到 {len(category_links)} 个分类链接")
        
        # 先处理新闻链接
        if news_links:
            print("处理新闻链接:")
            for item in news_links[:10]:  # 只处理前10个链接，避免过多请求
                print(f"  {item}")
                items = SpiderDataItem()
                items.article_url = item
                items.country = self.country
                items.keyword = self.extract_keyword_from_url(item)
                yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)
        
        # 处理分类链接
        if category_links:
            print("处理分类链接:")
            for category_link in category_links:
                print(f"  {category_link}")
                yield feapder.Request(category_link, callback=self.parse_category_page, meta={"page": 1})

        # 尝试获取下一页
        next_page_link = response.xpath("//a[contains(text(), 'Next') or contains(text(), 'next')]/@href").extract_first()
        if next_page_link and current_page < 5:  # 限制最大页数
            if next_page_link.startswith('http'):
                next_url = next_page_link
            elif next_page_link.startswith('/'):
                next_url = f"https://www.nyasatimes.com{next_page_link}"
            else:
                next_url = f"https://www.nyasatimes.com/{next_page_link}"
            print(f"下一页链接: {next_url}")
            yield feapder.Request(next_url, callback=self.parse_nyasatimes_home, meta={"page": current_page + 1})

    def extract_keyword_from_url(self, url):
        """从URL中提取关键词"""
        url_lower = url.lower()
        
        keywords_map = {
            'extreme': 'Extreme',
            'heat': 'Heat',
            'high temperature': 'High Temperature',
            'heavy rain': 'Heavy Rain',
            'drought': 'Drought',
            'power outage': 'Power Outage from Heat',
            'fire': 'Fire',
            'air pollution': 'Air Pollution',
            'climate change': 'Climate Change',
            'crop yield': 'Crop Yield Reduction',
            'oxygen deficiency': 'Oxygen Deficiency',
            'traffic': 'High Temperature Affecting Traffic',
            'ecological disaster': 'Ecological Disaster',
            'economy': 'Climate Change Affecting Economy',
            'marine heatwave': 'Marine Heatwave',
            'pollution': 'High Temperature Pollution',
            'coral': 'Coral'
        }
        
        for key, keyword in keywords_map.items():
            if key in url_lower:
                return keyword
        return ''

    def parse_category_page(self, request, response):
        current_page = request.meta.get("page", 1)
        print(f"当前分类页面页码: {current_page}")
        
        # 从分类页面提取新闻链接
        links = response.xpath("//a[@href]/@href").extract()
        
        # 构建完整链接
        clean_links = []
        for link in links:
            if link.startswith('http'):
                clean_links.append(link)
            elif link.startswith('/'):
                clean_links.append(f"https://www.nyasatimes.com{link}")
        
        # 去重
        clean_links = list(set(clean_links))
        
        # 过滤出新闻链接
        news_links = []
        for link in clean_links:
            # 排除非新闻链接
            if any(exclude in link for exclude in ['/category/', '/tag/', '/author/', '#', 'javascript:', 'mailto:', 'twitter.com', 'facebook.com', 'instagram.com', 'advertise', 'fact-check', 'about', 'contact']):
                continue
            # 包含新闻相关关键词的链接
            if any(keyword in link for keyword in ['/20', '/news/', '/article/', '-']):
                news_links.append(link)
        
        print(f"从分类页面找到 {len(news_links)} 个新闻链接")
        if news_links:
            for item in news_links[:10]:  # 只处理前10个链接，避免过多请求
                print(f"  {item}")
                items = SpiderDataItem()
                items.article_url = item
                items.country = self.country
                items.keyword = self.extract_keyword_from_url(item)
                yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)
        
        # 尝试获取分类页面的下一页
        next_page_link = response.xpath("//a[contains(text(), 'Next') or contains(text(), 'next') or contains(@class, 'next')]/@href").extract_first()
        if next_page_link and current_page < 3:  # 限制最大页数
            if next_page_link.startswith('http'):
                next_url = next_page_link
            elif next_page_link.startswith('/'):
                next_url = f"https://www.nyasatimes.com{next_page_link}"
            else:
                next_url = f"https://www.nyasatimes.com/{next_page_link}"
            print(f"分类页面下一页链接: {next_url}")
            yield feapder.Request(next_url, callback=self.parse_category_page, meta={"page": current_page + 1})
    
    def parse_search_results(self, request, response):
        current_page = request.meta.get("page", 1)
        search_keyword = request.meta.get("keyword", "")
        print(f"当前搜索结果页面页码: {current_page}，关键词: {search_keyword}")
        
        # 从搜索结果页面提取新闻链接
        links = response.xpath("//a[@href]/@href").extract()
        
        # 构建完整链接
        clean_links = []
        for link in links:
            if link.startswith('http'):
                clean_links.append(link)
            elif link.startswith('/'):
                clean_links.append(f"https://www.nyasatimes.com{link}")
        
        # 去重
        clean_links = list(set(clean_links))
        
        # 过滤出新闻链接
        news_links = []
        for link in clean_links:
            # 排除非新闻链接和搜索结果页面
            if any(exclude in link for exclude in ['/category/', '/tag/', '/author/', '#', 'javascript:', 'mailto:', 'twitter.com', 'facebook.com', 'instagram.com', 'advertise', 'fact-check', 'about', 'contact', '?s=']):
                continue
            # 包含新闻相关关键词的链接
            if any(keyword in link for keyword in ['/20', '/news/', '/article/', '-']):
                news_links.append(link)
        
        print(f"从搜索结果找到 {len(news_links)} 个新闻链接")
        if news_links:
            for item in news_links[:10]:  # 只处理前10个链接，避免过多请求
                print(f"  {item}")
                items = SpiderDataItem()
                items.article_url = item
                items.country = self.country
                items.keyword = search_keyword
                yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)
        
        # 尝试获取搜索结果的下一页
        next_page_link = response.xpath("//a[contains(text(), 'Next') or contains(text(), 'next') or contains(@class, 'next')]/@href").extract_first()
        if next_page_link and current_page < 3:  # 限制最大页数
            if next_page_link.startswith('http'):
                next_url = next_page_link
            elif next_page_link.startswith('/'):
                next_url = f"https://www.nyasatimes.com{next_page_link}"
            else:
                next_url = f"https://www.nyasatimes.com/{next_page_link}"
            print(f"搜索结果下一页链接: {next_url}")
            yield feapder.Request(next_url, callback=self.parse_search_results, meta={"keyword": search_keyword, "page": current_page + 1})

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        
        # 提取标题
        title = response.xpath("//h1/text()").extract_first()
        if not title:
            title = response.xpath("//title/text()").extract_first()
        items.title = title or ""
        
        # 提取内容
        content_selectors = [
            "//div[@class='nyasa-content']//p/text()",
            "//div[@class='content']//p/text()",
            "//article//p/text()",
            "//div[@class='article']//p/text()",
            "//div[@id='content']//p/text()",
            "//div[@class='post-content']//p/text()"
        ]
        
        content = ""
        for selector in content_selectors:
            extracted = response.xpath(selector).extract()
            if extracted:
                content = "".join(extracted).strip()
                if len(content) > 50:
                    break
        
        # 检查内容是否包含气候变化相关关键词
        contains_keyword = False
        matched_keyword = ""
        text_to_check = (title or "") + " " + (content or "").lower()
        
        # 定义与气候变化相关的上下文关键词
        climate_context_keywords = [
            'weather', 'climate', 'environment', 'temperature', 'rain', 'drought',
            'flood', 'storm', 'fire', 'pollution', 'global', 'warming',
            'greenhouse', 'carbon', 'emission', 'sustainability', 'ecology',
            'renewable', 'energy', 'natural', 'disaster', 'extreme', 'weather',
            'sea', 'level', 'ice', 'melt', 'ocean', 'acidification',
            'coral', 'bleaching', 'heatwave', 'cold', 'wave', 'flooding',
            'landslide', 'wildfire', 'air', 'quality', 'water', 'biodiversity',
            'conservation', 'climate', 'emergency', 'climate', 'action',
            'net', 'zero', 'carbon', 'neutral', 'renewable', 'energy'
        ]
        
        # 定义需要排除的上下文关键词（政治、体育等）
        exclude_context_keywords = [
            'political', 'politics', 'parliament', 'election', 'campaign',
            'government', 'minister', 'president', 'prime', 'minister',
            'party', 'vote', 'ballot', 'poll', 'candidate',
            'sports', 'football', 'soccer', 'basketball', 'tennis',
            'game', 'match', 'player', 'team', 'league',
            'business', 'finance', 'economy', 'market', 'stock',
            'company', 'corporation', 'profit', 'loss', 'revenue'
        ]
        
        # 先检查精确匹配的复合关键词
        for keyword in self.climate_keywords:
            if keyword.lower() in text_to_check:
                # 检查是否在正确的上下文中
                has_climate_context = any(context_keyword in text_to_check for context_keyword in climate_context_keywords)
                has_exclude_context = any(exclude_keyword in text_to_check for exclude_keyword in exclude_context_keywords)
                
                if has_climate_context and not has_exclude_context:
                    contains_keyword = True
                    matched_keyword = keyword
                    break
        
        # 如果没有找到精确匹配，检查基础关键词
        if not contains_keyword:
            # 提取基础关键词（去除重复）
            base_keywords = list(set(['heat', 'temperature', 'rain', 'drought', 'fire', 'pollution',
                                      'climate', 'disaster', 'weather', 'flood', 'storm', 'environment',
                                      'ecology', 'sustainability', 'energy', 'emission', 'carbon',
                                      'global warming', 'climate change', 'extreme weather', 'natural disaster']))
            
            for keyword in base_keywords:
                if keyword.lower() in text_to_check:
                    # 检查是否在正确的上下文中
                    has_climate_context = any(context_keyword in text_to_check for context_keyword in climate_context_keywords)
                    has_exclude_context = any(exclude_keyword in text_to_check for exclude_keyword in exclude_context_keywords)
                    
                    if has_climate_context and not has_exclude_context:
                        contains_keyword = True
                        matched_keyword = keyword
                        break
        
        items.content = content
        items.author = ''
        items.pubtime = ''
        items.keyword = matched_keyword
        
        print(f"获取新闻: {items.title}")
        if content and contains_keyword:
            print(f"  包含关键词: {matched_keyword}")
            yield items
        else:
            print("  不包含相关关键词，跳过")

if __name__ == "__main__":
    AirSpiderDemo().start()