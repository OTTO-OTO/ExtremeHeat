import feapder
from NewsItems import SpiderDataItem
from lxml import etree

class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=1,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[5, 8],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
    )
    country = 'Bangladesh'
    table = 'Bangladesh_thedailystar'
    keywords = ['Heat', 'High Temperature', 'Heavy Rain', 'Drought', 'Power Outage from Heat', 'Fire', 'Air Pollution',
                'Climate Change', 'Crop Yield Reduction', 'Oxygen Deficiency', 'High Temperature Affecting Traffic',
                'Ecological Disaster', 'Climate Change Affecting Economy', 'Marine Heatwave',
                'High Temperature Pollution', 'Coral']

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://www.google.com/search"
            params = {
                "q": f"{keyword} site:https://www.thedailystar.net/",
                "sca_esv": "77f8400c799712e6",
                "hl": "en",
                "sxsrf": "ADLYWILuCUJ3cOmlDuQG5d7yYr6LP0DWPA:1735623801501",
                "ei": "eYRzZ9uoHo3sg8UPm9yTkAk",
                "start": "0",
                "sa": "N",
                "sstk": "ATObxK4rKQMGRsJAKh3n6UfbCcCg6I1h5qlBbU_Q4xuzRH1divY6Xq68dLdDBtrfNsGtuUpRx1_a9nnuJcFyQEdHbtjnisDlUIqWCTqGKyr3CW8tqSJaDZiclYGxj_YahKHJ",
                "ved": "2ahUKEwibyZvBptGKAxUN9qACHRvuBJI4ChDy0wN6BAgIEAc",
                "biw": "1872",
                "bih": "268",
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
            "AEC": "AZ6Zc-UQGj3wkhlZM57vS7C6-GmtRo3QYitSSOXpURK5IHM1O_0bVt9JDg",
            "__Secure-1PSIDTS": "sidts-CjIB7wV3sTHiOlCZBwfBef4Som-mKr6BTMX8_jJaIZYJDVpOt1Glu_y-Q_IQ9HH9LovwNxAA",
            "__Secure-3PSIDTS": "sidts-CjIB7wV3sTHiOlCZBwfBef4Som-mKr6BTMX8_jJaIZYJDVpOt1Glu_y-Q_IQ9HH9LovwNxAA",
            "NID": "520=DAzPQRZsEPavcF3FGEEznj1jfNBygRbsXBrdxPCDJgVkJ9_qOFhfx9uxf5v8Wq0sgN7X7h3b6wlcZpDLysmfDWhrqQkSsGsLB98KziZjuhwgFpKdlbkkeRU39apgbyPzhuapppp3od8-33vTdemJAZFDRztCs_dFy7gHYI83P9OO4A8JyQrcPDRuTjJrjd8C54HO3yRZ63RKCXPwqvcHgKo2LVgpuPBpZljeKOLBzBqmgBUgnv1CbWqyxtNQKVYKqqpDOpZFrEF53HCYorUCziwTi_n9RF_jZWc4AnKQeUXpJXUbHsBJ_7VWJBDDK04jEHUYdC-9zp-VB7zaFlLwdqNCYmH3IfjOTZciEf5UCl2Bh9hX_jECQvoO-jZUVjj3tjJ7SxufsuFMPVqV-wO1SF8LM6MkgkQKWdjIEC3vi-Vmf8BcDY5-6qEgPdsLrt4CMFr6k_cA6rK3XM81JVEHuWQGuOEN9eWwm8p9UBtf_PiOEtwrOe0NDNjlPVB-KWqGUxD02da1FO3s3zhWXTKf4Nb00t4gc-JMCoHubaveA4dzrEmnKqgUeVrvfoWhRgmbp6dNEndzfwnlFG0zTilCiY8OKs_LOSyx2fzfushKbj2OoQNt03v_TE4AIIlmswivCeRXPKPvNkGcANn9ynznrLaGl9cdOwHejQO5_LTBKKkPMZqR9xMFtEc8kJEJseUUAEjUahuKxZtMjtW694fJxKmFTzPt961n_xhVIaj0frFr5NJHMfAuTWXbGllzox7H0M3EOLUqQSQNUFXgtxaYi2rT8VlOUpHLw9y6JAoxoA",
            "DV": "k1w5jRzDlCqi8JiFc16AWAZ-vdezQVmXimc7DQWIhQEAAMBLUjyomhJKCgEAANBSdRuTYlCKTQAAAHjU1yhfGuRCFAAAQF11u5an5KnVQ58AUP79Q69aOLsq0ScA9ELC8Uuvqk5e9AkA1vzqyIlUVPoafQLAVMew81FJaONGnwAA",
            "SIDCC": "AKEyXzVrS1xxh56xvrb_rXaGtmt2HAsqBX4_PeTt5Q-46WIF-Gktg_JSzfA1-8dBHEpar2n9WQ",
            "__Secure-1PSIDCC": "AKEyXzWyk9bVUm9o-49h_K1fKJrx5ZUa1HzMjriV0ZrN0eZKN-hKbM6L3vSfEDMRrfPtu9TOPsE",
            "__Secure-3PSIDCC": "AKEyXzXihbGQWG83bbVXasUJDs-l6T0w3hay42btAOu7eJnMo7yvm_HQbjqqK-iCdRDF5pm8d50"
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
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)


        current_page = request.page + 1
        url = "https://www.google.com/search"
        params = {
            "q": f"{current_keyword} site:https://www.thedailystar.net/",
            "sca_esv": "77f8400c799712e6",
            "hl": "en",
            "sxsrf": "ADLYWILuCUJ3cOmlDuQG5d7yYr6LP0DWPA:1735623801501",
            "ei": "eYRzZ9uoHo3sg8UPm9yTkAk",
            "start": f"{current_page*10}",
            "sa": "N",
            "sstk": "ATObxK4rKQMGRsJAKh3n6UfbCcCg6I1h5qlBbU_Q4xuzRH1divY6Xq68dLdDBtrfNsGtuUpRx1_a9nnuJcFyQEdHbtjnisDlUIqWCTqGKyr3CW8tqSJaDZiclYGxj_YahKHJ",
            "ved": "2ahUKEwibyZvBptGKAxUN9qACHRvuBJI4ChDy0wN6BAgIEAc",
            "biw": "1872",
            "bih": "268",
            "dpr": "1"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//article//div//p/text()").extract())
        items.author = ''
        items.pubtime = ''
        print(items)
        # if items.content:
        #     yield items

if __name__ == "__main__":
    AirSpiderDemo().start()
