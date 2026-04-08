import feapder


class AirSpiderDemo(feapder.AirSpider):
    def start_requests(self):
        url = "https://centralnoticias.gob.do/page/1/"
        params = {
            "s": "Calor"
        }
        yield feapder.Request(url, params=params, method="GET")

    def download_midware(self, request):
        request.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        request.cookies = {
            "_ga": "GA1.1.491074676.1734659205",
            "_ga_WZK0QBCXVD": "GS1.1.1735000590.2.1.1735000650.0.0.0"
        }
        return request

    def parse(self, request, response):
        print(response.text)
        print(response)


if __name__ == "__main__":
    AirSpiderDemo(thread_count=1).start()
