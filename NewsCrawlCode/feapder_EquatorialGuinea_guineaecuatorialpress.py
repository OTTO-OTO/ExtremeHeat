
import feapder
from NewsItems import SpiderDataItem
from curl_cffi import requests
from lxml import etree

class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[5, 8],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB="TropicalCyclone",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )

    )

    country = 'Equatorial_Guinea'
    table = 'Equatorial_Guinea'
    keywords = [ "Ola de calor extrema", "Temperatura alta", "Temperatura extrema", "Evento de ola de calor", "Aumento de la temperatura alta", "Impacto de la temperatura alta", "Temperatura alta", "Calor intenso", "Aumento de la temperatura", "Evento de calor", "Incremento de la temperatura", "Lluvias intensas", "Precipitación fuerte", "Lluvia torrencial", "Lluvias extremas", "Sequía", "Sequía severa", "Sequía prolongada", "Escasez de agua", "Corte de energía", "Corte de energía por alta temperatura", "Corte de energía por ola de calor", "Corte de energía causado por alta temperatura", "Incendio", "Incendio por temperatura alta", "Incendio por calor", "Incendio provocado por temperatura", "Incendio inducido por calor", "Impacto agrícola", "Ola de calor en la agricultura", "Daño a los cultivos", "Estrés térmico agrícola", "Hipoxia", "Golpe de calor", "Golpe de calor inducido por calor", "Hipoxia por temperatura alta", "Golpe de calor por alta temperatura", "Impacto en el tráfico", "Tráfico por alta temperatura", "Tráfico por ola de calor", "Tráfico por temperatura", "Desastre ecológico", "Desastre por calor", "Entorno de temperatura alta", "Impacto del calor en la biodiversidad", "Ecología de la ola de calor", "Contaminación", "Contaminación por alta temperatura", "Contaminación por calor", "Contaminación por temperatura", "Blanqueamiento de corales", "Arrecifes de coral por alta temperatura", "Blanqueamiento de corales por temperatura" ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.google.com/search"
            params = {
                "q": f"{keyword} site:guineaecuatorialpress.com",
                "sca_esv": "c182dbbc388e6267",
                "sxsrf": "ADLYWIKqig4Tbj-HWlA43BWVLTd5-eJlqQ:1736234701750",
                "ei": "zdZ8Z6q-LfWA0PEPp-6SiAQ",
                "start": "0",
                "sa": "N",
                "sstk": "ATObxK7Eow7G_LmvEg-a_Cll6rZNMgcKC5BI1JWrX6H6p-VIklsSYr88R1RovzCt_UqKx-0WD8nE0vDOdv-GSxUvyhVJlSObL9NxxA",
                "ved": "2ahUKEwjq2JOliuOKAxV1ADQIHSe3BEEQ8tMDegQIJBAE",
                "biw": "1872",
                "bih": "300",
                "dpr": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page,keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.google.com/",
            "sec-ch-prefers-color-scheme": "light",
            "sec-ch-ua": "\"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\", \"Google Chrome\";v=\"131\"",
            "sec-ch-ua-arch": "\"x86\"",
            "sec-ch-ua-bitness": "\"64\"",
            "sec-ch-ua-form-factors": "\"Desktop\"",
            "sec-ch-ua-full-version": "\"131.0.6778.205\"",
            "sec-ch-ua-full-version-list": "\"Chromium\";v=\"131.0.6778.205\", \"Not_A Brand\";v=\"24.0.0.0\", \"Google Chrome\";v=\"131.0.6778.205\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-model": "\"\"",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-ch-ua-platform-version": "\"10.0.0\"",
            "sec-ch-ua-wow64": "?0",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        }
        request.cookies = {
            "SID": "g.a000rggyq3rN6l5a4RzrqfKR8L6w_4oSKSt2-NpZPRMnn-bzQbIYlKeqO6xU_OhVrBsa_1wtYwACgYKAdsSARcSFQHGX2MiKv7DAe7dbGoquPBFfQzlghoVAUF8yKqr4O7h4A-W0Zzxi-B1MCbq0076",
            "__Secure-1PSID": "g.a000rggyq3rN6l5a4RzrqfKR8L6w_4oSKSt2-NpZPRMnn-bzQbIYIPYnTmEzPvYdlyJ5T8abmwACgYKAXgSARcSFQHGX2MiZdzCux5pDZfQbld3wiSXvhoVAUF8yKrE28tj1J7QUSxswbZGNPEd0076",
            "__Secure-3PSID": "g.a000rggyq3rN6l5a4RzrqfKR8L6w_4oSKSt2-NpZPRMnn-bzQbIYlvrEO6wyUoA2X7nR4dKaLQACgYKAc8SARcSFQHGX2Mi1LRifzVo7z1Yh0GO1dLi3BoVAUF8yKq57cjKXq68AYkR7eUXDFJW0076",
            "HSID": "A24n1xqEfBl88i-jZ",
            "SSID": "AlxzqkxJtB1hfXr5n",
            "APISID": "N8bORK8NqGLDFSzg/A6wAx4O94ZJV_yhma",
            "SAPISID": "IMadT4_VMCUqIjYe/AiQnZJDT7r7FsfPRc",
            "__Secure-1PAPISID": "IMadT4_VMCUqIjYe/AiQnZJDT7r7FsfPRc",
            "__Secure-3PAPISID": "IMadT4_VMCUqIjYe/AiQnZJDT7r7FsfPRc",
            "SEARCH_SAMESITE": "CgQI-JwB",
            "__Secure-ENID": "24.SE=o0yUUa3ZR9clSPYS5ExW_vXKoE2JZ41YqJURNK7mtHgsJfwqVj9FskxDJm2hGz4NJiUylI03Zdm6l66QaWLCDv-jUU0ojHCiQ5zdB2WO1HtgMo4XA_qMPoInfnCeBVYqIPFDuwytPnmGZzXB9kNYRByDmPez5jwiDfNkCq_a44QC6UzV_aAS-IAq_YRdwcvlXjAFHxOJcJxt4UgW",
            "AEC": "AZ6Zc-WyHP1QAO8pHDyrAlDd2TlXZikvfC7Pz7LVGreLs-AJs8g7gcqxQQ",
            "NID": "520=qlKQMvKBdgiKvslSrJAEsrQBAOv0l3ZaNziHMOV7z8Qu59oOpX8fwchcJQ3eSdNYwiKuZdTXDhSnAn18dJF2_hO6T3UVLQuYoIUh5QXLkePKdm6IygmRtWDkZOcpTLTXwWIZIB3Ld_6MJz66uG2kaQ3wmKoDn52KAychEC0d5qbmJVd1xane5hl5eMSXRoj37-bhrV1FsMMGpNy8oNlw1sr_I22Z5KWYK362NfIfHE_7xvomiPjnJT9vrTJrsAV-6ZOTryBiruXgxeaAcUGPsK1a8ryfWQ946ca_bAKGvzMTIhTxXp39B3gICoKS_9AwVoi5Km4PfK56C3o_DQRFANIG_FfkQHSy_0RSTaiDx1xFHkBur9hdAO43en6mn0tV7wYKOSnqMS1l9TkjnCK11Fn7dbNayt66yXBDVyjjnbAqmM4ha_8EHBXoIv-DEoRJv4Zr4JEhvuWd8gq4UdXgzLIA9wiM0QtraWw5aZhMke6VaD1rGkKXAzKOsseu5hppZwyyyz11ff50Y1Rpa1GXJSXv_haMZt13atWmTk5vHUnAkUm_iLARod3t6EBeq8GCiVsbzKDAnE0In4lMWNEaDwlOELGsHYL4hHOfjJxusBScofJzsXZiPhj7xmfrFp21uWVg4Tzw44z7ECossU8OHCST_gfXZXdq88-47ccZ_LARTHFxKcYRcv7Uc2GNFeLvUZKQk4pbkC-Ip4XG_5IvAx5gVOgGJj5IMvfMD7w5vvl5KlD3EdVv6KJguDnzqySd5Fz7nBbvIKxJvU_PSzlcfnuHK81noKZXw16J7t5HqQ",
            "DV": "k1w5jRzDlCpSUFfdruUpeWolOs75Q9lUx7DzUUloYwAAANALCccvvao6OQAAAFjzqyMnUlHpEwAAAOXfP_SqhbOrBgAAAA",
            "__Secure-1PSIDTS": "sidts-CjIB7wV3sfnipWwnvwAYnLiDodk2aIcZmbrfIm9_ctC78OZZUbXC1HrMsynX2nzseKiSNxAA",
            "__Secure-3PSIDTS": "sidts-CjIB7wV3sfnipWwnvwAYnLiDodk2aIcZmbrfIm9_ctC78OZZUbXC1HrMsynX2nzseKiSNxAA",
            "SIDCC": "AKEyXzVH5dZjm3mK_Sb9locdxznAA3Q9vSp0NZaE2htv2nBhGhFtCH5_q7v0UqzhFfTcKobD9A",
            "__Secure-1PSIDCC": "AKEyXzVWapFhLAadbEQoPHsI02wt6BTU0VgouUjNgXPDfZm5V4s44BS13JXfrsqYswgzp_PXeN0",
            "__Secure-3PSIDCC": "AKEyXzVEWWeWZzXgE_kz87Mj4OdQ0kz2_bAnymrh_oWGDHAJamfeQ4aOQaI_wzJLax0XxtV3vL8"
        }
        return request


    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//span[@jscontroller='msmzHf']/a/@href").extract()
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            print(item)
            items = SpiderDataItem()
            items.article_url = item
            items.country = self.country
            items.keyword = request.keyword # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.google.com/search"
        params = {
            "q": f"{current_keyword} site:guineaecuatorialpress.com",
            "sca_esv": "c182dbbc388e6267",
            "sxsrf": "ADLYWIKqig4Tbj-HWlA43BWVLTd5-eJlqQ:1736234701750",
            "ei": "zdZ8Z6q-LfWA0PEPp-6SiAQ",
            "start": f"{current_page * 10}",
            "sa": "N",
            "sstk": "ATObxK7Eow7G_LmvEg-a_Cll6rZNMgcKC5BI1JWrX6H6p-VIklsSYr88R1RovzCt_UqKx-0WD8nE0vDOdv-GSxUvyhVJlSObL9NxxA",
            "ved": "2ahUKEwjq2JOliuOKAxV1ADQIHSe3BEEQ8tMDegQIJBAE",
            "biw": "1872",
            "bih": "300",
            "dpr": "1"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='article_text']//p//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items

if __name__ == "__main__":
    AirSpiderDemo().start()
