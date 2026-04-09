# -*- coding: utf-8 -*-
import json
import re
import time
import uuid

import feapder
from feapder import Item


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[8, 10],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB="importance_country_data",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )

    previous_links = None
    country = 'Nicaragua'
    table = 'Nicaragua'
    keywords = ["calor" ,"Ola de calor extrema", "Temperatura alta", "Temperatura extrema", "Evento de ola de calor", "Aumento de la temperatura alta", "Impacto de la temperatura alta", "Temperatura alta", "Calor intenso", "Aumento de la temperatura", "Evento de calor", "Incremento de la temperatura", "Lluvias intensas", "Precipitación fuerte", "Lluvia torrencial", "Lluvias extremas", "Sequía", "Sequía severa", "Sequía prolongada", "Escasez de agua", "Corte de energía", "Corte de energía por alta temperatura", "Corte de energía por ola de calor", "Corte de energía causado por alta temperatura", "Incendio", "Incendio por temperatura alta", "Incendio por calor", "Incendio provocado por temperatura", "Incendio inducido por calor", "Impacto agrícola", "Ola de calor en la agricultura", "Daño a los cultivos", "Estrés térmico agrícola", "Hipoxia", "Golpe de calor", "Golpe de calor inducido por calor", "Hipoxia por temperatura alta", "Golpe de calor por alta temperatura", "Impacto en el tráfico", "Tráfico por alta temperatura", "Tráfico por ola de calor", "Tráfico por temperatura", "Desastre ecológico", "Desastre por calor", "Entorno de temperatura alta", "Impacto del calor en la biodiversidad", "Ecología de la ola de calor", "Contaminación", "Contaminación por alta temperatura", "Contaminación por calor", "Contaminación por temperatura", "Blanqueamiento de corales", "Arrecifes de coral por alta temperatura", "Blanqueamiento de corales por temperatura" ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://confidencial.digital/page/1/"
            params = {
                "s": f"{keyword}"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://confidencial.digital/?s=calor",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-arch": "\"x86\"",
            "sec-ch-ua-bitness": "\"64\"",
            "sec-ch-ua-full-version": "\"131.0.2903.146\"",
            "sec-ch-ua-full-version-list": "\"Microsoft Edge\";v=\"131.0.2903.146\", \"Chromium\";v=\"131.0.6778.265\", \"Not_A Brand\";v=\"24.0.0.0\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-model": "\"\"",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-ch-ua-platform-version": "\"10.0.0\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        request.cookies = {
            "_lr_retry_request": "true",
            "_lr_env_src_ats": "false",
            "hb_insticator_uid": "ac76c294-0ee6-443a-aada-f21e36133234",
            "_gid": "GA1.2.470348199.1737093690",
            "gcid_first": "189ab6c1-d0d8-4cc5-867b-55edc0221d0e",
            "cf_clearance": "MMsN0F_3jSDTMEG1rY5WCtTP0f0cykecfIO6okzghHA-1737093675-1.2.1.1-IlbkOTrJTrsPjPQEf.0NABO9DRIU8qPQqFPYlRE_atq5HrEIgDNIO8EiK36E6B7ncV97JwPFJtCW6lFT_aGn85NDCgYf644XjI7.VBurs_AsBx76bmy.LgJwCFrK8x37987xwzqrsJ2LkVtD10Qo2QOu59bwL_oaTLh8S6e4FlaSORYtP9cmb8KPdrLltnNGaCiQ6fcp5fTsxM7QSKAddR0j2WLNIGwDu.6yQ7P_Vgr4fRJqsuBYRdY_s21YGvEgWy4irTs4av1Fvavszkg0oBSjYNZYzmFqbEPBE9d5277l.u8dM.uGFXz3FyL_uA3l_WGEOxYKPnjZOymEhcDOPQ",
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "panoramaId_expiry": "1737698476036",
            "panoramaId": "c2ba4404dd1662779c299379ee5816d53938f08b6896985ba839d86735aa250d",
            "panoramaIdType": "panoIndiv",
            "_hjSession_2519920": "eyJpZCI6ImY2YjdkOTM1LTU0MGItNGY5Mi1hOWU3LTM2NTAxYjNiMzg1NiIsImMiOjE3MzcwOTM2OTM4NzYsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MH0=",
            "__qca": "P0-1925360103-1737093694319",
            "_ga_C49PKYGF8J": "GS1.1.1737093688.1.1.1737093782.58.0.0",
            "_ga": "GA1.2.184373387.1737093689",
            "_gat_UA-13285749-1": "1",
            "_hjSessionUser_2519920": "eyJpZCI6ImRlM2U0NDVkLWJkOWUtNWZjMC04ODU0LWU0MDEyODAzM2I0NCIsImNyZWF0ZWQiOjE3MzcwOTM2OTM4NzUsImV4aXN0aW5nIjp0cnVlfQ==",
            "_ga_QXPK198T2V": "GS1.1.1737093688.1.1.1737093785.40.0.0",
            "cto_bundle": "YWh98F9PT1JQUkJIcFlMNFJhcloxd0pnNGdUT1BzOGg0YWdncGFXUGFUWkRnNCUyQkRXOUJjODhHRGlkVHJ4NndvOFN6UGQyWHlRdmd4TXJ5UlNXMkhYZjJLT3Y5QlZUTm5mRmNGNkxWSG9xZ0xRcTJWYzRES2VneG9kUnFnN0JDZXdMTHYxUSUyQnhybE4lMkJ0M3clMkI1OFRrYnpKcWxOcE1lOXdDZ0IwdkoxSGhIZTlLbkloVSUzRA"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//article/div/h2/a/@href").extract()
        # print(json.loads(links))
        # # 输出匹配的值
        # for match in matches:
        #     print(match)

        # print(links)
        current_page = request.page

        current_links = links
        if self.previous_links is not None and current_links == self.previous_links:
            print(f"关键词 {current_keyword} 的第 {current_page} 页链接与上一页相同，退出当前关键字的循环")
            return None  # 如果链接相同，返回 None 表示结束当前关键词的处理

        self.previous_links = current_links  # 更新上一页的链接列表

        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            items = Item()
            items.table_name = self.table
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            # items.pubtime = item.get("recs-publishtime")
            items.keyword = current_keyword
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://confidencial.digital/page/{current_page}/"
        params = {
            "s": f"{current_keyword}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='content']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
