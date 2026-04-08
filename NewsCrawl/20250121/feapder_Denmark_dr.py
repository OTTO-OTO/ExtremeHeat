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

    country = 'Denmark'
    table = 'Denmark_dr'
    keywords = [
    "Varm", "Ekstrem varmebølge", "Høj temperatur", "Ekstreme temperaturer", "Varmebølge-hændelse", "Stigende høj temperatur", "Effekter af høj temperatur", "Høj temperatur", "Intens varme", "Temperaturstigning", "Varmehændelse", "Temperaturhøjere", "Intens nedbør", "Stærk nedbør", "Tunge regn", "Ekstrem nedbør", "Tørke", "Alvorlig tørke", "Langvarig tørke", "Vandsmangel", "Strømnedslukning", "Høj temperatur strømnedslukning", "Varmebølge strømnedslukning", "Høj temperatur fører til strømnedslukning", "Brand", "Høj temperatur brand", "Varmebølge-brand", "Temperatur-brand", "Høj temperatur udløser brand", "Landbrugs effekter", "Varmebølge landbrug", "Skadede afgrøder", "Landbrug varme stress", "Svært at ånde", "Hed", "Varmehed", "Høj temperatur svært at ånde", "Høj temperatur hed", "Trafik effekter", "Høj temperatur trafik", "Varmebølge trafik", "Temperatur trafik", "Økologisk katastrofe", "Varmekatastrofe", "Høj temperatur miljø", "Varmebølge påvirker biodiversitet", "Varmebølge økologi", "Forurening", "Høj temperatur forurening", "Varmeforurening", "Temperaturforurening", "Koralbleikning", "Høj temperatur koraller", "Temperaturbleikning koraller"
]

    def start_requests(self):
        for keyword in self.keywords:
            page = 0
            url = "https://www.dr.dk/tjenester/steffi/graphql"
            params = {
                "query": "fragment ArticleResultFields on Article { type: __typename urn urlPathId title format publications { ...on ArticlePublication { breaking live serviceChannel { urn } } } summary startDate teaserImage { default { url } } head { type: __typename ... on MediaComponent { resource { type: __typename ... on LiveMedia { urn mediaType } ... on Clip { urn mediaType durationInMilliseconds } ... on ClipBundle { items(limit: 1) { __typename urn durationInMilliseconds } } } } ... on ImageCollectionComponent { images { default: image(key: \"default\") { type: __typename } } } ... on RatingComponent { rating } } contributions(limit: 1) { agent { ... on Person { name } } role } site { url urn title presentation { isShortFormat colors teaserImage { default: image(key: \"default\") { url } } } } } fragment RecipeResultFields on Recipe { title url image { url } startDate } fragment UnknownSearchResultFields on UnknownSearchResult { title url label image { url width height } } fragment MusicArtistResultFields on MusicArtist { url name image { url width height } } query SearchPageDRDK($query: String! $limit: Int! $offset: Int! $sort: SearchSort) { drdk: search(query: $query logQuery: $query limit: $limit offset: $offset products: [\"drdk\"] sort: $sort) { totalCount results: nodes { type: __typename ... on Article { ...ArticleResultFields } ... on Recipe { ...RecipeResultFields } ... on MusicArtist { ...MusicArtistResultFields } ... on UnknownSearchResult { ...UnknownSearchResultFields } } spellCheck } }",
                "variables": f'{{"query":"{keyword}","sort":"Relevance","limit":10,"offset":0}}'

            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "priority": "u=1, i",
            "x-application-name": "hydra-prod",
            "User-Agent": "Apifox/1.0.0 (https://apifox.com)",
            "Accept": "*/*",
            "Host": "www.dr.dk",
            "Connection": "keep-alive"
        }
        request.cookies = {
            "CookieConsent": "{stamp:%27LcJxgXEHEbgOPLs33XTtnB0oL9hC3Z7PKA8ek+h+Xxvr86z80KzLZA",
            "ajs_anonymous_id": "2e349237-7100-4a1d-b1cc-6d1b98601908",
            "lang": "da",
            "ss": "1",
            "cf-fe": "s%3AAnonymous2%2Cg%3Adrtv%7Coptedout",
            "RT": "\"z"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = response.json['data'].get("drdk").get("results")
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
            items.table_name = self.table # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url ="https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://www.dr.dk/tjenester/steffi/graphql"
        params = {
            "query": "fragment ArticleResultFields on Article { type: __typename urn urlPathId title format publications { ...on ArticlePublication { breaking live serviceChannel { urn } } } summary startDate teaserImage { default { url } } head { type: __typename ... on MediaComponent { resource { type: __typename ... on LiveMedia { urn mediaType } ... on Clip { urn mediaType durationInMilliseconds } ... on ClipBundle { items(limit: 1) { __typename urn durationInMilliseconds } } } } ... on ImageCollectionComponent { images { default: image(key: \"default\") { type: __typename } } } ... on RatingComponent { rating } } contributions(limit: 1) { agent { ... on Person { name } } role } site { url urn title presentation { isShortFormat colors teaserImage { default: image(key: \"default\") { url } } } } } fragment RecipeResultFields on Recipe { title url image { url } startDate } fragment UnknownSearchResultFields on UnknownSearchResult { title url label image { url width height } } fragment MusicArtistResultFields on MusicArtist { url name image { url width height } } query SearchPageDRDK($query: String! $limit: Int! $offset: Int! $sort: SearchSort) { drdk: search(query: $query logQuery: $query limit: $limit offset: $offset products: [\"drdk\"] sort: $sort) { totalCount results: nodes { type: __typename ... on Article { ...ArticleResultFields } ... on Recipe { ...RecipeResultFields } ... on MusicArtist { ...MusicArtistResultFields } ... on UnknownSearchResult { ...UnknownSearchResultFields } } spellCheck } }",
            "variables": f'{{"query":"{current_keyword}","sort":"Relevance","limit":10,"offset":{current_page * 10}}}'
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        # items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='dre-article-body']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
