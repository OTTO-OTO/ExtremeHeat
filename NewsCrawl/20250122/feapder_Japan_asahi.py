# -*- coding: utf-8 -*-
"""

本地运行

"""
import re

import feapder
from feapder import Item
import json
from lxml import etree


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[8, 10],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB="other_country_site2",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Japan'
    table = 'Japan_asahi'
    keywords =[
    "熱", "極端な熱波", "高温", "極端な温度", "熱波イベント", "高温の増加", "高温の影響", "高温", "強い熱", "温度の上昇",
    "熱イベント", "温度の上昇", "強降雨", "強降水", "豪雨", "極端な降雨", "干ばつ", "重度の干ばつ", "長期の干ばつ",
    "水資源の不足", "停電", "高温による停電", "熱波による停電", "高温が原因の停電", "火災", "高温による火災", "熱による火災",
    "温度による火災", "高温が引き起こす火災", "農業への影響", "熱波農業", "作物の損害", "農業の熱ストレス", "酸欠",
    "熱中症", "高温による酸欠", "高温による熱中症", "交通への影響", "高温交通", "熱波交通", "温度交通", "生態災害",
    "熱災害", "高温環境", "熱が生物多様性に与える影響", "熱波生態", "汚染", "高温汚染", "熱汚染", "温度汚染", "サンゴの白化",
    "高温サンゴ礁", "温度白化サンゴ"
]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://sitesearch.asahi.com/sitesearch-api/"
            params = {
                "Keywords": f"{keyword}",
                "start": "0",
                "sort": ""
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "$cookie": "_gtm_digick=1; _gcl_au=1.1.183108290.1737516857; pbjs_sharedId=ed2933fe-48a8-4683-9a89-2c4a89202189; pbjs_sharedId_cst=zix7LPQsHA%3D%3D; panoramaId_expiry=1738121644538; _cc_id=a9d614ff34c6df511157c622a7f6ca67; panoramaId=c2ba4404dd1662779c299379ee5816d53938f08b6896985ba839d86735aa250d; _ga=GA1.1.121676224.1737516875; AMCVS_D16360625419F1800A4C98A2%40AdobeOrg=1; sc_clk_btn=no%20value; sc_prv_mp=nm; s_lv=1737516876170; s_lv_s=First%20Visit; s_cc=true; _yjsu_yjad=1737516876.97662c0d-dd97-48c2-b528-d2a7a9eebb8c; _cb=vATIDBieMX5B66TVy; _chartbeat2=.1737516877909.1737516877909.1.DImt2qCNhxQPBEj4SXDta2PeB29sSq.1; _cb_svref=external; __lt__cid.060f9758=f90e7a22-0635-4e9e-9333-97b15f52f3f9; __lt__sid.060f9758=176f6c35-2b61492c; _uetsid=d45022e0d87111efb66f218ce2e7e5b5; _uetvid=d45039b0d87111ef8a998fdc6d43d0c7; AMCV_D16360625419F1800A4C98A2%40AdobeOrg=-1303530583%7CMCIDTS%7C20111%7CMCMID%7C43565545693583557833559785075799168974%7CMCAAMLH-1738121676%7C3%7CMCAAMB-1738121676%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1737524076s%7CNONE%7CMCSYNCSOP%7C411-20118%7CvVersion%7C3.3.0; cto_bundle=PaXXZF9PT1JQUkJIcFlMNFJhcloxd0pnNGdYUlZMMTU3S3ViZGtYRXpMZVF0clVGJTJCYW9WcERXZmhaeFNyNENaUEZoTVZVT1FEQ2VXaENaYUZqSXFJRFBVdXRNUWUyR3VHTEQ3ajRsR3VKSjJhSTd1akI0N1p3R1poVWd6U24lMkJYMTdSQktVOHFITTRLNTFZTk9tVndDZWxyV3pRJTNEJTNE; _a1_f=db6bacdf-bbbe-4ecd-8c34-47f49ca05c46; _a1_sync=\\u0021adb|1738726493655; _pctx=%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAEzIBYB2AVh4AYAzIICM-AEyC%2BInhwAcHHiAC%2BQA; _pcid=%7B%22browserId%22%3A%22m67cp6r2xgk98nm1%22%7D; cX_P=m67cp6r2xgk98nm1; _tt_enable_cookie=1; _ttp=XESyV8xB-NmyuiCdchUNrSEUqrd.tt.1; _fbp=fb.1.1737516904101.251402943769955254; cX_G=cx%3A31z6una0ikqyb1e0evmqj50xjm%3A3eubn60rdwu46; __pid=.asahi.com; __tbc=%7Bkpex%7DiYI71oTK_KR5hnJvs7hwOQVOpZE0h3bCO2gLwAsX9RV95-xqmIiOvDu9lxSf0yaR; __pat=32400000; __pvi=eyJpZCI6InYtbTY3Y3A3anE3d2dqbG5mMCIsImRvbWFpbiI6Ii5hc2FoaS5jb20iLCJ0aW1lIjoxNzM3NTE2OTExMTExfQ%3D%3D; xbc=%7Bkpex%7D6fFO3nUV6EnLb8nKbhCOKQ; com.silverpop.iMAWebCookie=3da67e52-72ec-b4ec-6660-0ee3515288d4; com.silverpop.iMA.session=51113334-8464-4360-7a73-4365a30b07d2; __hstc=124200789.75d28a855c2f78f5087b5eea17fb176b.1737516915403.1737516915403.1737516915403.1; hubspotutk=75d28a855c2f78f5087b5eea17fb176b; __hssrc=1; __hssc=124200789.1.1737516915403; sc_m_attr=s_uk%3Ab_uk; _chartbeat4=t=DFVeJ9C4U6mMCTllZ3C33bGQBR0fZT&E=23&x=0&c=1.65&y=11372&w=447; _ga_MCVDNZMSYQ=GS1.1.1737516874.1.1.1737516982.54.0.0; sc_ppv_pagename=%2Fsitesearch%2Findex.html%5Bsitesearch.asahi.com%5D; sc_ppv_v78=%2Fsitesearch%2Findex.html%5Bsitesearch.asahi.com%5D; _dtm_lastline=no%20value; _td=7c164ccb-38af-4261-8b07-f2b52496b941; com.silverpop.iMA.page_visit=514088657:47:; ASAHISEG=AS0%3D0; s_pnum=https%3A%2F%2Fsitesearch.asahi.com%2Fsitesearch%2F%3FKeywords%3D%25E9%25AB%2598%25E6%25B8%25A9%26Searchsubmit2%3D%25E6%25A4%259C%25E7%25B4%25A2%26Searchsubmit%3D%25E6%25A4%259C%25E7%25B4%25A2%26iref%3Dpc_ss_date_btn2%26sort%3D%26start%3D20%26s_vn%3D3%26non_tgt%3D1; s_tp=5476; s_ppv=%2Fsitesearch%2Findex.html%255Bsitesearch.asahi.com%255D%2C96%2C8%2C5247; s_nr=1737517022929-New; s_sq=asahicomall%3D%2526pid%253D%25252Fsitesearch%25252Findex.html%25255Bsitesearch.asahi.com%25255D%2526pidt%253D1%2526oid%253Dfunctioncn%252528%252529%25257B%25257D%2526oidt%253D2%2526ot%253DA; _ga=GA1.3.121676224.1737516875; _gid=GA1.3.150601510.1737517023; _gat_UA-6624382-1=1",
            "priority": "u=1, i",
            "referer": "https://sitesearch.asahi.com/sitesearch/?Keywords=%E9%AB%98%E6%B8%A9&Searchsubmit2=%E6%A4%9C%E7%B4%A2&Searchsubmit=%E6%A4%9C%E7%B4%A2&iref=pc_ss_date_btn3&sort=&start=40",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json['goo'].get('docs')

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
            print(item)
            items.table_name = self.table  # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url =item.get("URL")
            items.title = item.get("TITLE")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = item.get("ReleaseDate")
            items.author = ''
            items.content = item.get("BODY")
            print(items)
            # yield items
            # yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 20
        url = "https://sitesearch.asahi.com/sitesearch-api/"
        params = {
            "Keywords": f"{current_keyword}",
            "start": f"{current_page}",
            "sort": ""
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    # def parse_detail(self, request, response):
    #     items = request.items
    #     # items.title = response.xpath("//h1/text()").extract_first()
    #     items.content = "".join(response.xpath("//div[@class='article-content ']//p/text()").extract())
    #
    #     items.author = ''
    #     items.pubtime = ''
    #     print(items)
    #     if items.content:
    #         yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
