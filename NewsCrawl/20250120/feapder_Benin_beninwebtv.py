# -*- coding: utf-8 -*-
# 173
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

    country = 'Benin'
    table = 'Benin_beninwebtv'
    keywords =[ "Chaud","Vague de chaleur extrême", "Température élevée", "Température extrême", "Événement de vague de chaleur", "Augmentation de la température élevée", "Impact de la température élevée",  "Chaleur intense", "Augmentation de la température", "Événement de chaleur", "Augmentation de la température", "Pluies intenses", "Précipitations fortes", "Pluie torrentielle", "Pluies extrêmes", "Sécheresse", "Sécheresse sévère", "Sécheresse prolongée", "Pénurie d'eau", "Panne de courant", "Panne de courant par température élevée", "Panne de courant par vague de chaleur", "Panne de courant causée par température élevée", "Incendie", "Incendie par température élevée", "Incendie par chaleur", "Incendie provoqué par température", "Incendie induit par chaleur", "Impact agricole", "Vague de chaleur en agriculture", "Dommages aux cultures", "Stress thermique agricole", "Hypoxie", "Coup de chaleur", "Coup de chaleur induit par chaleur", "Hypoxie par température élevée", "Coup de chaleur par température élevée", "Impact sur le trafic", "Trafic par température élevée", "Trafic par vague de chaleur", "Trafic par température", "Catastrophe écologique", "Catastrophe par chaleur", "Environnement de température élevée", "Impact de la chaleur sur la biodiversité", "Écologie de la vague de chaleur", "Pollution", "Pollution par température élevée", "Pollution par chaleur", "Pollution par température", "Blanchiment des coraux", "Récifs coralliens par température élevée", "Blanchiment des coraux par température" ]


    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://beninwebtv.com/wp-admin/admin-ajax.php"
            params = {
                "td_theme_name": "Newspaper",
                "v": "12.6.8"
            }
            data = {
                "action": "td_ajax_block",
                "td_atts": f'''{{"ajax_pagination": "infinite", "limit": "10", "cloud_tpl_module_id": "335912",
                                            "modules_on_row": "", "search_query": "{keyword}", "installed_post_types": "", "sort": "",
                                            "block_type": "tdb_flex_loop_builder", "separator": "", "custom_title": "", "custom_url": "",
                                            "block_template_id": "", "title_tag": "", "f_header_font_header": "",
                                            "f_header_font_title": "Block header", "f_header_font_settings": "", "f_header_font_family": "",
                                            "f_header_font_size": "", "f_header_font_line_height": "", "f_header_font_style": "",
                                            "f_header_font_weight": "", "f_header_font_transform": "", "f_header_font_spacing": "",
                                            "f_header_": "", "offset": "", "post_ids": "-153389", "include_cf_posts": "",
                                            "exclude_cf_posts": "", "ajax_pagination_infinite_stop": "", "pag_space": "", "pag_padding": "",
                                            "pag_border_width": "", "pag_border_radius": "", "prev_tdicon": "", "next_tdicon": "",
                                            "pag_icons_size": "", "pag_text": "", "pag_h_text": "", "pag_a_text": "", "pag_bg": "",
                                            "pag_h_bg": "", "pag_a_bg": "", "pag_border": "", "pag_h_border": "", "pag_a_border": "",
                                            "f_pag_font_title": "Pagination text", "f_pag_font_settings": "", "f_pag_font_family": "",
                                            "f_pag_font_size": "", "f_pag_font_line_height": "", "f_pag_font_style": "",
                                            "f_pag_font_weight": "", "f_pag_font_transform": "", "f_pag_font_spacing": "", "f_pag_": "",
                                            "modules_gap": "48", "all_modules_space": "", "modules_horiz_align": "flex-start",
                                            "modules_vert_align": "flex-start", "modules_padding": "", "modules_bg": "", "modules_bg_h": "",
                                            "m_shadow_shadow_header": "", "m_shadow_shadow_title": "Shadow", "m_shadow_shadow_size": "",
                                            "m_shadow_shadow_offset_horizontal": "", "m_shadow_shadow_offset_vertical": "",
                                            "m_shadow_shadow_spread": "", "m_shadow_shadow_color": "",
                                            "m_shadow_h_shadow_title": "Hover shadow", "m_shadow_h_shadow_size": "",
                                            "m_shadow_h_shadow_offset_horizontal": "", "m_shadow_h_shadow_offset_vertical": "",
                                            "m_shadow_h_shadow_spread": "", "m_shadow_h_shadow_color": "", "all_m_bord": "",
                                            "all_m_bord_style": "", "all_m_bord_color": "", "m_bord_color_h": "", "m_bord_radius": "",
                                            "all_divider": "", "all_divider_style": "solid", "all_divider_color": "", "el_class": "",
                                            "tdc_css": "", "td_column_number": 1, "header_color": "", "td_ajax_preloading": "",
                                            "td_ajax_filter_type": "", "td_filter_default_txt": "", "td_ajax_filter_ids": "",
                                            "color_preset": "", "ajax_pagination_next_prev_swipe": "", "border_top": "", "css": "",
                                            "class": "tdi_53", "tdc_css_class": "tdi_53", "tdc_css_class_style": "tdi_53_rand_style"}}''',
                "td_block_id": "tdi_53",
                "td_column_number": "1",
                "td_current_page": "1",
                "block_type": "tdb_flex_loop_builder",
                "td_filter_value": "",
                "td_user_action": "",
                "td_magic_token": "fe8a7df7fc"
            }
            yield feapder.Request(url, params=params,data=data, callback=self.parse_url, page=page, keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://beninwebtv.com",
            "priority": "u=1, i",
            "referer": "https://beninwebtv.com/?s=Chaud",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"131\", \"Chromium\";v=\"131\", \"Not_A Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "x-requested-with": "XMLHttpRequest"
        }
        request.cookies = {
            "__utmc": "35789747",
            "__utmz": "35789747.1737181017.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)",
            "__utma": "35789747.1545906495.1737181017.1737184273.1737351364.3",
            "_clck": "is6oz4%7C2%7Cfsq%7C0%7C1844",
            "__utmt": "1",
            "__utmb": "35789747.7.10.1737351364",
            "__gads": "ID=376faff0c1d6be9f:T=1737181003:RT=1737352300:S=ALNI_MbiwsXmJ_khmAFe89LrUi6DKwmA4A",
            "__gpi": "UID=00000fc8941e877e:T=1737181003:RT=1737352300:S=ALNI_Mati0vEFGEhStIyiK6wi0OwO_TQMA",
            "__eoi": "ID=bcb75effae623be0:T=1737181003:RT=1737352300:S=AA-AfjYTLhmkJe956l_DyY0KIAu1",
            "_clsk": "1t6zds9%7C1737352316431%7C8%7C1%7Ck.clarity.ms%2Fcollect",
            "cto_bundle": "9K7OgF9PT1JQUkJIcFlMNFJhcloxd0pnNGdmYUxVNW5GNVRXQWMxMlZUekJ2a3VkbWFoZkg1YlNJSSUyRnBoMWs0Y25NOE91U0FuUFJLenRaMlpKRFZYN2pnWjlUajc3d1I4WFA3YzFpNUlwZkdWTnVaTWI4SWJKemlmMmJ1dzBrdkVLaWxodURRaHYzZ204ZFNSazNrJTJGOVhBdzF3JTNEJTNE",
            "cto_bidid": "_BlYIV9RTExMNFlMY3JtQzY2UUhodUg1TmN6RVRBQ2JXOUFrM054TnJXZmFrVDQxUVZobUtydzRta2oxNFFSTEc3Sm9lZDI0Nktlb21LRGdybTdLYVJ6aCUyRm1FU3FVd0Z4Wmg2dmpPa2p5V2dPSU1vJTJCU1JGa3lxNWklMkZaMlIyRnVBanJKTQ",
            "cto_dna_bundle": "odRUk19PT1JQUkJIcFlMNFJhcloxd0pnNGdSZjV4Y3BGUGg5dk5mZU01djluS0glMkJKUzQ4QmYzM2xMTm9rVk9LbEVQaHNIaUJWa24lMkY1MjFiSjhqRHVNcFhSVUElM0QlM0Q",
            "FCNEC": "%5B%5B%22AKsRol8nCFzjslOulCtaIbd7Ro4Vu3Mb9WpBQscuX_nprhe8OLwDIjGd7WksXLCoq5wYARpiVmJLHjNkEusMXJC9jGzDj1IyY_0cegkb-775Xv5E7mY4hm9e4xeZi1v8RGxYO91ndD7yCHt1__j6LNc7I-xonfkZRQ%3D%3D%22%5D%5D"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")

        data = response.json['td_data']
        from lxml import etree
        html = etree.HTML(data)
        links = html.xpath("//h3/a/@href")
        print(links)
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
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://beninwebtv.com/wp-admin/admin-ajax.php"
        params = {
            "td_theme_name": "Newspaper",
            "v": "12.6.8"
        }
        data = {
            "action": "td_ajax_block",
            "td_atts": f'''{{"ajax_pagination": "infinite", "limit": "10", "cloud_tpl_module_id": "335912",
                                "modules_on_row": "", "search_query": "{current_keyword}", "installed_post_types": "", "sort": "",
                                "block_type": "tdb_flex_loop_builder", "separator": "", "custom_title": "", "custom_url": "",
                                "block_template_id": "", "title_tag": "", "f_header_font_header": "",
                                "f_header_font_title": "Block header", "f_header_font_settings": "", "f_header_font_family": "",
                                "f_header_font_size": "", "f_header_font_line_height": "", "f_header_font_style": "",
                                "f_header_font_weight": "", "f_header_font_transform": "", "f_header_font_spacing": "",
                                "f_header_": "", "offset": "", "post_ids": "-153389", "include_cf_posts": "",
                                "exclude_cf_posts": "", "ajax_pagination_infinite_stop": "", "pag_space": "", "pag_padding": "",
                                "pag_border_width": "", "pag_border_radius": "", "prev_tdicon": "", "next_tdicon": "",
                                "pag_icons_size": "", "pag_text": "", "pag_h_text": "", "pag_a_text": "", "pag_bg": "",
                                "pag_h_bg": "", "pag_a_bg": "", "pag_border": "", "pag_h_border": "", "pag_a_border": "",
                                "f_pag_font_title": "Pagination text", "f_pag_font_settings": "", "f_pag_font_family": "",
                                "f_pag_font_size": "", "f_pag_font_line_height": "", "f_pag_font_style": "",
                                "f_pag_font_weight": "", "f_pag_font_transform": "", "f_pag_font_spacing": "", "f_pag_": "",
                                "modules_gap": "48", "all_modules_space": "", "modules_horiz_align": "flex-start",
                                "modules_vert_align": "flex-start", "modules_padding": "", "modules_bg": "", "modules_bg_h": "",
                                "m_shadow_shadow_header": "", "m_shadow_shadow_title": "Shadow", "m_shadow_shadow_size": "",
                                "m_shadow_shadow_offset_horizontal": "", "m_shadow_shadow_offset_vertical": "",
                                "m_shadow_shadow_spread": "", "m_shadow_shadow_color": "",
                                "m_shadow_h_shadow_title": "Hover shadow", "m_shadow_h_shadow_size": "",
                                "m_shadow_h_shadow_offset_horizontal": "", "m_shadow_h_shadow_offset_vertical": "",
                                "m_shadow_h_shadow_spread": "", "m_shadow_h_shadow_color": "", "all_m_bord": "",
                                "all_m_bord_style": "", "all_m_bord_color": "", "m_bord_color_h": "", "m_bord_radius": "",
                                "all_divider": "", "all_divider_style": "solid", "all_divider_color": "", "el_class": "",
                                "tdc_css": "", "td_column_number": 1, "header_color": "", "td_ajax_preloading": "",
                                "td_ajax_filter_type": "", "td_filter_default_txt": "", "td_ajax_filter_ids": "",
                                "color_preset": "", "ajax_pagination_next_prev_swipe": "", "border_top": "", "css": "",
                                "class": "tdi_53", "tdc_css_class": "tdi_53", "tdc_css_class_style": "tdi_53_rand_style"}}''',
            "td_block_id": "tdi_53",
            "td_column_number": "1",
            "td_current_page": f"{current_page}",
            "block_type": "tdb_flex_loop_builder",
            "td_filter_value": "",
            "td_user_action": "",
            "td_magic_token": "fe8a7df7fc"
        }
        yield feapder.Request(url, params,data=data,callback=self.parse_url, page=current_page, keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='wpb_wrapper']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
