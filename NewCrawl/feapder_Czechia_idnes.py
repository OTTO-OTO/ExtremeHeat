import feapder
from feapder import Item
from lxml import etree
import re


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[6, 8],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB="other_country_site1",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    country = 'Czechia'
    table = 'Czechia_idnes'
    keywords = [
        'Extrémní', 'Vysoká teplota', 'Vysoká teplota', 'Silný dešť', 'Sucho',
        'Výpad elektrické energie kvůli vysoké teplotě', 'Požár', 'Ovzdušné znečištění', 'Klimatické změny',
        'Snížení produkce plodin', 'Oxygenní nedostatek', 'Vysoká teplota ovlivňuje dopravu',
        'Ekologická katastrofa', 'Klimatické změny ovlivňují ekonomiku', 'Mořská vlnová vlna',
        'Znečištění vysokou teplotou', 'Korál'
    ]
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://hledej.idnes.cz/"
            params = {
                "q": f"{keyword}",
                "strana": "1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Referer": "https://hledej.idnes.cz/?q=Extr%e9mn%ed",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\""
        }
        request.cookies = {
            "personalizace": "setver=full&sp=3145239663523415",
            "_webid": "3.a653bb0d26.1734594366.1734594366",
            "_mmid": "lqe1ed121ec1866c",
            "sso": "id=---&_v=o60bjtyabb&e=1766098800",
            "euconsent-v2": "CQJ4dQAQJ4dQAAHABBENBSFgAP_gAEPgAAAAJqIBJC5kBSFCAGJgYNkAIAAWxxAAIAAAABAAgAAAABoAIAgAEAAwAAQABAAAABAAIEAAAABACABAAAAAQAAAAQAAAAAQAAAAAQAAAAAAAiBACAAAAABAAQAAAABAQAAAgAAAAAIAQAAAAAAAgAAAAAAAAAAAABAAAQgAAAAAAAAAAAAAAAAAAAAAAAAAABBAAAAAAAAAAAAAAAAAwTUgQAAVAAuABwAEAAMgAaABEACYAFUALgAYgA_ACEgEQARIAjgBlwDuAO8AfoBBwCLAElANoAdQBNoCpAFZALUAW4AvMBkgDUwJqADBIAMAAQVUHQAYAAgqoSgAwABBVQpABgACCqhCADAAEFVC0AGAAIKqAAAA.f_wAH_wAAAAA",
            "_pcid": "%7B%22browserId%22%3A%22lqe1ed121ec1866c%22%7D",
            "__pat": "3600000",
            "_ga": "GA1.1.1775733584.1734590807",
            "__io_r": "idnes.cz",
            "__io_first_source": "idnes.cz",
            "__io_lv": "1734590807830",
            "__io": "216f4bbbe.722d20867_1734590807830",
            "__io_unique_41571": "19",
            "_fbp": "fb.1.1734590810057.47395772410473200",
            "cX_P": "lqe1ed121ec1866c",
            "cX_G": "cx%3A1dibybclz7rev1fnijjclvsc6r%3A1i0znw55c7j64",
            "_pbjs_userid_consent_data": "4402978728844048",
            "_cc_id": "a9d614ff34c6df511157c622a7f6ca67",
            "pbjs-unifiedid": "%7B%22TDID%22%3A%22933953a3-aafa-4469-8f2b-04016b4ae9e9%22%2C%22TDID_LOOKUP%22%3A%22FALSE%22%2C%22TDID_CREATED_AT%22%3A%222024-12-19T06%3A46%3A56%22%7D",
            "udid": "pQ6X2U7u7kzwzdXEw9Ic2a5-IIgznpNk@1734590822114@1734590822114",
            "_ga_Q0N1FHWKWZ": "GS1.1.1734591917.1.0.1734591917.60.0.0",
            "_webid2": "89095219.1735800794033.I",
            "webidsync": "1735800806150",
            "euctmp": "CQJ4dQAQJ4dQAAHABBENBSFgAP_gAEPgAAAAJqIBJC5kBSFCAGJgYNkAIAAWxxAAIAAAABAAgAAAABoAIAgAEAAwAAQABAAAABAAIEAAAABACABAAAAAQAAAAQAAAAAQAAAAAQAAAAAAAiBACAAAAABAAQAAAABAQAAAgAAAAAIAQAAAAAAAgAAAAAAAAAAAABAAAQgAAAAAAAAAAAAAAAAAAAAAAAAAABBAAAAAAAAAAAAAAAAAwTUgQAAVAAuABwAEAAMgAaABEACYAFUALgAYgA_ACEgEQARIAjgBlwDuAO8AfoBBwCLAElANoAdQBNoCpAFZALUAW4AvMBkgDUwJqADBIAMAAQVUHQAYAAgqoSgAwABBVQpABgACCqhCADAAEFVC0AGAAIKqAAAA.f_wAH_wAAAAA",
            "lastAdSyncDate": "1735800806154",
            "__tbc": "%7Bkpex%7DvtCyMwObsw2NtUr48_pzgpuSKRT7pQAnbhUbfMikGnnxYfMmp2hPpRlBum4tUf6v",
            "__pvi": "eyJpZCI6InYtbTVleXplMDVleDZreGM5dCIsImRvbWFpbiI6Ii5pZG5lcy5jeiIsInRpbWUiOjE3MzU4MDA4MDc5NzV9",
            "xbc": "%7Bkpex%7Db8KtEPaX3z020bd-pbLRHIiqjEFlGjHixMGx5O7HXHVvXYApApVOBQfMyRt4nXDUVN0MXrx509pxK4yDueWiDc2prZbdfIJQv9dhaM1ZzqzaAh30UOp0zxgnaynSPo4W-vULie5I7aqdleiO9V84q3BQ2O3nZg-wOqeFmT9cdVw",
            "sso_sync": "idnes=1735800806142",
            "myId5": "ID5*n9dAPMJ4n1hbmumWdGL9xzF2rIIlJ-mWdGL9xzF2rILmU60419TMUZoAuTSkNgf65Jd9WKCTewkFpAGnIu_88-Sni7nxxSXb1NxAYwdcBP7krkfCDmP5XaILDcnDODZ-5MM4kqPUQk5gPQkXTJ25HeTY_8Ifm6hfHr0MxKREpU4",
            "__io_d": "1_705468254",
            "__io_pr_utm_campaign": "%7B%22referrerHostname%22%3A%22docs.qq.com%22%7D",
            "__io_session_id": "0ed3b743d.a395965af_1735800798373",
            "__io_nav_state41571": "%7B%22current%22%3A%22%2F%22%2C%22currentDomain%22%3A%22www.idnes.cz%22%2C%22previousDomain%22%3A%22%22%7D",
            "__gfp_64b": "iL2MA4he1QY8zchuuhh7JXUKSYTqIqyS2qPFmLvMdM..P7|1734590781|2|||8:1:80",
            "_pctx": "%7Bu%7DN4IgrgzgpgThIC5QCYAMBOAHJ1yAseAzMACYC2AFgL6KgAOMUAZgJYAeiI5FIANCABcAnnSicAwgA0QVfmiw58RYAEcWAdxpIQDZu05r1fQSLEIQUmbPDQYAZQEBDAZE6OAdgHt3xiCwFQAJIknHgA7ITIAKx4yJiEYQBsePFhyAl4MkA",
            "lastPianoSyncDate": "1735800806155",
            "_ga_162BQEC208": "GS1.1.1735800795.2.1.1735800893.0.0.0",
            "_ga_G0ZTTLYP2W": "GS1.1.1735800795.2.1.1735800893.60.0.0",
            "kolbda": "0",
            "dCMP": "mafra=1111,all=1,reklama=1,part=0,cpex=1,google=1,gemius=1,id5=1,next=0000,onlajny=0000,jenzeny=0000,databazeknih=0000,autojournal=0000,skodahome=0000,skodaklasik=0000,groupm=1,piano=1,seznam=1,geozo=0,czaid=1,click=1,verze=2,",
            "sid": "id=7268800566987449710|t=1734590821.920|te=1735800903.252|c=208F9CD3A695BE7CE21CF7320B9F78C8",
            "panoramaId_expiry": "1736405703693",
            "panoramaId": "0010a56387b454fb99733e574a5e16d539386232f10af484e6cddb3c256de10d",
            "cto_bidid": "QQy7118wVjJXRTBrZm8zWlR3U050Tzc0UnB2Q0l6M0J6Y1Y2SUVqVzNlYVc3cW1tNk55U1NsbElva1hpamRNWjRmeEM3ZHdxTUJOT1NBYzdnek53dWh1cmQyMiUyRkU5ck9TRSUyQldzdnh5blM4eWpMQTglM0Q",
            "cto_bundle": "cm2xC19PT1JQUkJIcFlMNFJhcloxd0pnNGdReHRqOVVFS0VOTTVhdEt6T01Wc2pRMEg5Rzg3OU10OWx0MHNZbTJCM0RzSTNWQUVDM3o4UFpmUWVaOVRsT3E3MkFma0prQkhlU3l2YzZLYmQ3RHVkSVN3V2FIM1pnSDVzN1gzbDhadE13NXdORUVEM3lrZ2JhSlQ5V0N5WjBZbVElM0QlM0Q",
            "cto_dna_bundle": "FI00tV9PT1JQUkJIcFlMNFJhcloxd0pnNGdSZjV4Y3BGUGg5dk5mZU01djluS0glMkJKUzQ4QmYzM2xMTm9rVk9LbEVQaHNpV0JoejhVSzZmbHBKeTREOFVycWRBJTNEJTNE"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.xpath("//a[@class='art-link']/@href").extract()
        if not links:
            print(f"关键词 {current_keyword} 的第 {request.page} 页没有数据，退出当前关键字的循环")
            return None  # 如果没有数据，返回 None 表示结束当前关键词的处理
        for item in links:
            print(item)
            items = Item()
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://hledej.idnes.cz/"
        params = {
            "q": f"{current_keyword}",
            "strana": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='art-full']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
