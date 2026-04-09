import feapder
from NewsItems import SpiderDataItem


class AirSpiderDemo(feapder.AirSpider):
    country = 'Netherlands'
    table = 'Netherlands'
    # 英语
    keywords = ["Tropical cyclone", "Tropical depression", "Tropical storm", "Typhoon", "Hurricane", "Cyclone", "Storm", "Heavy rain", "Flood", "Surge", "Coastal damage", "Slide", "Geological disaster", "Marine disaster", "Strong winds", "Typhoon disaster", "Mudslide", "Landslide"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1  # 确保每个关键词循环开始时page重置为1
            url = "https://nos.nl/api/search"
            params = {
                "q": f"{keyword}",
                "page": '1'
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "baggage": "sentry-environment=production,sentry-release=1551713ed4a8f4e07f42e67de4468173fbb1acdd,sentry-public_key=3caf4fd1080f42859504caf5189fa266,sentry-trace_id=f72b5072fcec4cf9aebc511ba908877c,sentry-sample_rate=0,sentry-transaction=%2Fzoeken,sentry-sampled=false",
            "priority": "u=1, i",
            "referer": "https://nos.nl/zoeken?q=heat&page=2",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "sentry-trace": "f72b5072fcec4cf9aebc511ba908877c-8ba1cb860f4df9ce-0",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "pa_privacy": "%22optin%22",
            "_pcid": "%7B%22browserId%22%3A%22m5521m9gc6i2ce3c%22%2C%22_t%22%3A%22mktgz3lj%7Cm5521m9j%22%7D",
            "_pctx": "%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAE0RXSwH18yBbANb4A5gC8AzKkEAffgFZ5AJgCM-AJyCQAXyA",
            "nos_dark_mode": "",
            "_sotmpid": "0:m5521nfs:4iQHgMryVxmglFLNq7VaPPNr~K3JTM9T",
            "CCM_ID": "uvY4wBSX2lFmiU2m",
            "Cookie_Category_Necessary": "true",
            "Cookie_Category_Analytics": "true",
            "Cookie_view": "1",
            "Cookie_Consent": "Thu Dec 26 2024 16:24:16 GMT+0800",
            "_ain_uid": "1735353384022.471794552.9011234",
            "nos_preferences": "nos_preferences"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        data = response.json.get("items")
        if not data:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in data:
            print(item)
            if item.get("type") == 'article':
                items = SpiderDataItem()
                items.article_url = "https://nos.nl/artikel/" + item.get("id")
                items.title = item.get("title")
                items.country = self.country
                items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
                yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://nos.nl/api/search"
        params = {
            "q": f"{current_keyword}",
            "page": f'{current_page}'
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.content = "".join(response.xpath("//main//div//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
