import feapder
from feapder import Item


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[6, 10],
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
    previous_links = None

    country = 'Spain'
    table = 'Spain'
    #西班牙语
    keywords = ["Ciclón tropical", "Depresión tropical", "Tormenta tropical", "Tifón", "Huracán", "Ciclón", "Tormenta", "Lluvias intensas", "Inundación", "Marea", "Daños costeros", "Deslizamiento", "Desastre geológico", "Desastre marino", "Vientos fuertes", "Desastre de tifones", "Derrumbes de lodo", "Deslizamiento de tierra"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.20minutos.es/busqueda/1/"
            params = {
                "q": f"{keyword}",
                "category": "",
                "articleTypes%5B0%5D": "",
                "excludedArticleTypes%5B0%5D": "mam"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.20minutos.es/busqueda/?q=Temperatura%20alta",
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
            "gig_bootstrap_3_Hob3SXLWsBRwjPFz4f4ZEuxPo2PyeElFMWgWO2EXdBYUqwMk8lxNj78Fi_2VW5cH": "gigya_ver4",
            "didomi_token": "eyJ1c2VyX2lkIjoiMTk0NGU0MzEtYWJmYS02NDIyLTg4YWMtOGI5YzZkN2QwYjVmIiwiY3JlYXRlZCI6IjIwMjUtMDEtMTBUMDM6MzA6MDguMTkxWiIsInVwZGF0ZWQiOiIyMDI1LTAxLTEwVDAzOjMwOjEyLjEwNloiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwiYzpiaW5nLWFkcyIsImM6c3RpY2t5YWRzIiwiYzp0cmliYWwtZnVzaW9uIiwiYzpjaGFydGJlYXQiLCJjOmdpZ3lhLWNvdW50ZXIiLCJjOmFkNG1hdCIsImM6Z2lneWEiLCJjOmNsaWNrb25vbWV0cmljcyIsImM6dmlzaWJsZS1tZWFzdXJlcyIsImM6c3luYWNvciIsImM6dmVuZG9yLWlvIiwiYzp2aWRlb2xvZ3kiLCJjOmdvb2dsZWFuYS00VFhuSmlnUiIsImM6YXdzLWNsb3VkZnJvbnQiLCJjOnNtYXJ0bWVhbi1UYzZCWXhQcCIsImM6YWRvdG1vYiIsImM6YWRzY2FsZSIsImM6dmVuZG9yLWRvZ3RyYWNrIiwiYzpzY29vdGEtRVZDd3J5Q2QiLCJjOmhhdmFzbWVkaS1CQUc3cEpEZSIsImM6dmVuZG9yLXByb21ldGVvIiwiYzp1dGlxc2FuLUVZVndDbUtaIiwiYzpmcmFjdGlvbmFsLW4yallERlBOIiwiYzppZ25pdGlvbm8tTFZBTVpkbmoiXX0sInB1cnBvc2VzIjp7ImVuYWJsZWQiOlsiZ2VvbG9jYXRpb25fZGF0YSIsImRldmljZV9jaGFyYWN0ZXJpc3RpY3MiLCJ1c29kZWRpci1ORWo0cmlMTSJdfSwidmVuZG9yc19saSI6eyJlbmFibGVkIjpbImdvb2dsZSIsImM6dmlkZW9sb2d5IiwiYzpzY29vdGEtRVZDd3J5Q2QiLCJjOmhhdmFzbWVkaS1CQUc3cEpEZSIsImM6ZnJhY3Rpb25hbC1uMmpZREZQTiIsImM6aWduaXRpb25vLUxWQU1aZG5qIl19LCJ2ZXJzaW9uIjoyLCJhYyI6IkhwZUR5QUVZQUtZQVdRQXVnQnNBRDBBSThBU29BeEFCbEFEbUFKdUFVTUF3SUJpZ0RQZ0htZ1BjQTk0Q0hBRWZBSkxBYU9BOVVDRFlFS3dJaUFSSEFpU0JGTUNSd0VvZ0phZ1NfQW5vQlB3Q2lvRmh3TEdnV3BBdllCaFlERVFHVUFNNWdaMUExR0JyQURYSUc5Z09KQWNqQTZjQjFZRHNJSGZRUEVnZjNCQXdDQm9FRlFJT3dRcmdpQkJGUUNMRUVab0pEQVNNQWtiQkpxQ1ZNRXRZSmNBVFVBbTlCT0tDZFVFNzBLQndvSUJRWUNnOEZDSUtQUVVyQXBiQlRJQ25VRmZvTERnV1FnczdCWjlDMElMUXdXakF0SkJha0M1UUZ6b0xxZ1hrZ3ZRQmZrREJjR0RnTUpRWWNBdzZoaWNHSjRNYmdZNGd4NkJrV0RKTUdVUU0zQVoxQXo4Qm9BRUNNSTJZZWx3QS5IcGVEeUFFWUFLWUFXUUF1Z0JzQUQwQUk4QVNvQXhBQmxBRG1BSnVBVU1Bd0lCaWdEUGdIbWdQY0E5NENIQUVmQUpMQWFPQTlVQ0RZRUt3SWlBUkhBaVNCRk1DUndFb2dKYWdTX0Fub0JQd0Npb0Zod0xHZ1dwQXZZQmhZREVRR1VBTTVnWjFBMUdCckFEWElHOWdPSkFjakE2Y0IxWURzSUhmUVBFZ2YzQkF3Q0JvRUZRSU93UXJnaUJCRlFDTEVFWm9KREFTTUFrYkJKcUNWTUV0WUpjQVRVQW05Qk9LQ2RVRTcwS0J3b0lCUVlDZzhGQ0lLUFFVckFwYkJUSUNuVUZmb0xEZ1dRZ3M3Qlo5QzBJTFF3V2pBdEpCYWtDNVFGem9McWdYa2d2UUJma0RCY0dEZ01KUVljQXc2aGljR0o0TWJnWTRneDZCa1dESk1HVVFNM0FaMUF6OEJvQUVDTUkyWWVsd0EifQ==",
            "euconsent-v2": "CQLA94AQLA94AAHABBENBXFsAP_gAEPgAAiQJ2NH_C7fbWlr8X53YfsEcY0P5dh55sQxBgbBA-IFDJKQsJwWhmAxJAzgNKAKGBIAMmRBIQMlGIBQQUAAYIgBKQDMKAyQIBAIIiAAiAIBAwJICAgrCgggMQAIgGBEEFUAmAgBQFJoGNgAggBChSCRIAAAAICAAwCoAFEgRAEYAQAIQCxAAwIAqAwAAEsAABkoEQABAAACIoACAAAABRAAQTeAJMFCogBLAkJCCQMIIAAIgiCAigQBAAAkDBAQAGCAhQBgAKMBAAAAAAAgQAAAABEACAAACAACAAIACgQAAQAAQABgAQCAQAEAAAAACAAAAABAUABBAAgACAACIAKgBAhCAACAAMKAAAAABSAEAAAACAQEAAAAAAABAAAIAAAARAACEAAQBUAAAAAAAAAAQAAACAAAABAAAAAAAACAAAAA.f_wACHwAAAAA",
            "___nrbic": "%7B%22isNewUser%22%3Atrue%2C%22previousVisit%22%3A1736479807%2C%22currentVisitStarted%22%3A1736479807%2C%22sessionId%22%3A%22127ce705-d9fa-415a-86c2-d3297a1615ad%22%2C%22sessionVars%22%3A%5B%5B%22marfeelPassNoticeShown%22%2C%22true%22%5D%2C%5B%22marfeelPassConsentAccepted%22%2C%22true%22%5D%5D%2C%22visitedInThisSession%22%3Atrue%2C%22pagesViewed%22%3A1%2C%22landingPage%22%3A%22https%3A//www.20minutos.es/%22%2C%22referrer%22%3A%22%22%2C%22lpti%22%3Anull%7D",
            "___nrbi": "%7B%22firstVisit%22%3A1736479807%2C%22userId%22%3A%22a38682b1-ce66-4e49-b720-a19df8fb2053%22%2C%22userVars%22%3A%5B%5B%22mrfExperiment_Test%20A/B/C/D%22%2C%223%22%5D%2C%5B%22mrfExperiment_Test%20A/B%22%2C%222%22%5D%2C%5B%22mrfExperiment_PersonalizedTest%22%2C%222%22%5D%2C%5B%22mrfExperiment_Test%20A/B/C%22%2C%221%22%5D%5D%2C%22futurePreviousVisit%22%3A1736479807%2C%22timesVisited%22%3A1%2C%22userType%22%3A%220%22%7D",
            "compass_uid": "a38682b1-ce66-4e49-b720-a19df8fb2053",
            "_scor_uid": "c7192a75b2c44cc1b1c5d6ea64c4a378",
            "sui_1pc": "173647981214174C945D8D91122F91E95BB0882173F2DCB8B5ADA0E2",
            "_ga": "GA1.1.1104539985.1736479812",
            "_fbp": "fb.1.1736479813519.31693805994982510",
            "_clck": "13j4d44%7C2%7Cfsg%7C0%7C1836",
            "_SUIPROMETEO": "10858528-d2f2-452c-875d-45f1319c8c6f",
            "_lr_geo_location": "SG",
            "_lr_retry_request": "true",
            "_lr_env_src_ats": "false",
            "_lr_sampling_rate": "100",
            "pbjs-unifiedid": "%7B%22TDID%22%3A%223c63d331-14a9-431d-be77-36f718506495%22%2C%22TDID_LOOKUP%22%3A%22FALSE%22%2C%22TDID_CREATED_AT%22%3A%222025-01-10T03%3A30%3A08%22%7D",
            "pbjs-unifiedid_cst": "6CwELOosag%3D%3D",
            "g_state": "{\"i_p\":1736487068993,\"i_l\":1}",
            "permutive-id": "0b849104-f47a-4096-beca-d5cdddd98ced",
            "_ga_99PYP6EBJE": "GS1.1.1736479812.1.1.1736479893.59.0.0",
            "__bs_id": "GA1.1.1104539985.1736479812",
            "_tfpvi": "NTI1ZTIyMGMtOWYxMy00YTI3LTk4NzktNDhkMzRlZjBmYWFjIzMw",
            "_clsk": "3rcj96%7C1736479894197%7C3%7C0%7Cf.clarity.ms%2Fcollect",
            "cto_bundle": "axAYnl9PT1JQUkJIcFlMNFJhcloxd0pnNGdUeDljdzd4MGpveWsxekQlMkI0N1FJWHRwT2UlMkJFR0hha3ZXQkoyT3FWMGFSNlhLWlJoS013d3V2WXU2czBscVc1c1Z5eTZiaEhHVmxoTng1WTY3UUR6SEZVUGdKdkp6cUl0RTM3Wm0lMkJrNHFBM2lLTHY3bTFTeTElMkJzSFpBNFpTVk5uQSUzRCUzRA"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//article//h1/a/@href").extract()
        # print(data)

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
            # print(item.get("url"))
            items = Item()
            items.table_name = self.table
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = f"https://www.20minutos.es/busqueda/{current_page}/"
        params = {
            "q": f"{current_keyword}",
            "category": "",
            "articleTypes%5B0%5D": "",
            "excludedArticleTypes%5B0%5D": "mam"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//title/text()").extract_first()
        content = "".join(response.xpath("//p[@class='paragraph']/text()").extract())
        items.content = content
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
