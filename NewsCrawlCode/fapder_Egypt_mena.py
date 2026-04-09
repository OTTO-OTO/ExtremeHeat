import feapder
from NewsItems import SpiderDataItem
from lxml import etree
import re


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[5, 8],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
    )
    country = 'Egypt'
    table = 'Egypt_mena'
    # keywords = [
    #     'Extremo', 'Calor', 'Alta Temperatura', 'Lluvia Intensa', 'Sequía',
    #     'Corte de Energía por Calor', 'Incendio', 'Contaminación del Aire',
    #     'Cambio Climático',
    #     'Reducción de la Producción Agrícola', 'Deficiencia de Oxígeno',
    #     'Golpe de Calor', 'Calor Afectando el Tráfico', 'Desastre Ecológico',
    #     'Cambio Climático Afectando la Economía', 'Ola de Calor Marina',
    #     'Contaminación por Alta Temperatura', 'Coral'
    # ]
    keywords = [
    'حرارة قصوى',
    'حرارة مرتفعة',
    'درجة حرارة عالية',
    ' أمطار شديدة',
    'جفاف',
    'انقطاع الكهرباء بسبب الحرارة',
    'نار',
    ' ôلوان الهواء',
    'تغير المناخ',
    'انخفاض إنتاج المحاصيل',
    'نقص الأكسجين',
    'تأثير الحرارة على المرور',
    'كارثة بيئية',
    'تأثير تغير المناخ على الاقتصاد',
    'موجة حرارة في المحيط',
    'ôلوان الهواء بسبب الحرارة',
    'قرنيط'
]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = f"https://www.mena.org.eg/search/quick/keywords/{keyword}/page/1"
            yield feapder.Request(url, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.mena.org.eg/search/quick",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "LanguageFront": "ar",
            "__utma": "173448639.941722103.1735868859.1735868859.1735868859.1",
            "__utmc": "173448639",
            "__utmz": "173448639.1735868859.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)",
            "__utmt": "1",
            "AWSALB": "w65T0viFoqSnwYn+btyy8wTMO/t/SJp0YJazy5P9kLcwH1Jibl8E8pQEMiox5Gp3WT7w0now1PAuewcyaPtG6EbkSAI1Gp0axNpabSmRYtlNOSccFwYqZLoVb1Ug",
            "AWSALBCORS": "w65T0viFoqSnwYn+btyy8wTMO/t/SJp0YJazy5P9kLcwH1Jibl8E8pQEMiox5Gp3WT7w0now1PAuewcyaPtG6EbkSAI1Gp0axNpabSmRYtlNOSccFwYqZLoVb1Ug",
            "__utmb": "173448639.5.10.1735868859"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//div[@class='MenaResultTitle']/a/@href").extract()[:10]
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            print(item)
            items = SpiderDataItem()
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://www.mena.org.eg/search/quick/keywords/{current_keyword}/page/{current_page}"
        yield feapder.Request(url, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='entry-content']//p/text()").extract())
        items.author = ''
        items.pubtime = ''
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
