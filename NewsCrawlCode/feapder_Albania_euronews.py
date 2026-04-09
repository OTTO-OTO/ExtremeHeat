# -*- coding: utf-8 -*-
"""

本地运行

"""
import re
from datetime import datetime

import feapder
from feapder import Item
import json
import time
from lxml import etree
from sympy import print_tree


class AirSpiderDemo(feapder.AirSpider):
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[8, 10],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB="other_country_site3",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )
    previous_links = None

    country = 'Albania'
    table = 'Albania_euronews'
    keywords = ["heavy rain"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://euronews.al/en/wp-admin/admin-ajax.php"
            params = {
                "td_theme_name": "Newspaper",
                "v": "12.6.6"
            }
            data = {
                "action": "td_ajax_block",
                "td_atts": f"{{\"search_query\":\"{keyword}\",\"modules_on_row\":\"\",\"hide_audio\":\"yes\",\"category_id\":\"\",\"image_width\":\"eyJhbGwiOiI0NyIsInBvcnRyYWl0IjoiNDAiLCJwaG9uZSI6IjEwMCJ9\",\"image_floated\":\"eyJhbGwiOiJmbG9hdF9sZWZ0IiwicGhvbmUiOiJub19mbG9hdCJ9\",\"image_radius\":\"\",\"image_height\":\"eyJhbGwiOiI2NSIsImxhbmRzY2FwZSI6IjcwIiwicG9ydHJhaXQiOiI3MCIsInBob25lIjoiNjAifQ==\",\"meta_info_horiz\":\"\",\"modules_category\":\"\",\"modules_category_margin\":\"eyJhbGwiOiIycHggMTBweCAwIDAiLCJsYW5kc2NhcGUiOiIycHggOHB4IDAgMCIsInBvcnRyYWl0IjoiMnB4IDhweCAwIDAiLCJwaG9uZSI6IjJweCA4cHggMCAwIn0=\",\"show_author\":\"none\",\"tdc_css\":\"eyJhbGwiOnsibWFyZ2luLWJvdHRvbSI6IjAiLCJkaXNwbGF5IjoiIn0sImxhbmRzY2FwZSI6eyJkaXNwbGF5IjoiIn0sImxhbmRzY2FwZV9tYXhfd2lkdGgiOjExNDAsImxhbmRzY2FwZV9taW5fd2lkdGgiOjEwMTksInBvcnRyYWl0Ijp7ImRpc3BsYXkiOiIifSwicG9ydHJhaXRfbWF4X3dpZHRoIjoxMDE4LCJwb3J0cmFpdF9taW5fd2lkdGgiOjc2OCwicGhvbmUiOnsiZGlzcGxheSI6IiJ9LCJwaG9uZV9tYXhfd2lkdGgiOjc2N30=\",\"meta_padding\":\"eyJhbGwiOiIwIDAgMCA0MnB4IiwibGFuZHNjYXBlIjoiMCAwIDAgMzRweCIsInBvcnRyYWl0IjoiMCAwIDAgMjRweCIsInBob25lIjoiMThweCAwIDAifQ==\",\"meta_info_align\":\"center\",\"f_title_font_family\":\"522\",\"f_title_font_weight\":\"700\",\"f_title_font_size\":\"eyJhbGwiOiIyOCIsImxhbmRzY2FwZSI6IjI0IiwicG9ydHJhaXQiOiIyMiIsInBob25lIjoiMjQifQ==\",\"f_title_font_line_height\":\"1.2\",\"title_txt_hover\":\"#017df4\",\"cat_bg\":\"rgba(255,53,53,0)\",\"cat_bg_hover\":\"rgba(255,53,53,0)\",\"cat_txt\":\"#002e44\",\"date_txt\":\"#727277\",\"f_meta_font_size\":\"eyJhbGwiOiIxMyIsImxhbmRzY2FwZSI6IjEyIiwicG9ydHJhaXQiOiIxMiIsInBob25lIjoiMTIifQ==\",\"f_cat_font_size\":\"eyJhbGwiOiIxMyIsImxhbmRzY2FwZSI6IjEyIiwicG9ydHJhaXQiOiIxMiIsInBob25lIjoiMTIifQ==\",\"f_cat_font_line_height\":\"1\",\"f_meta_font_line_height\":\"1\",\"modules_category_padding\":\"0\",\"cat_txt_hover\":\"#000000\",\"com_bg\":\"#eaeaea\",\"com_txt\":\"#727277\",\"art_title\":\"eyJhbGwiOiIwIDAgMTBweCIsImxhbmRzY2FwZSI6IjAgMCA4cHgiLCJwaG9uZSI6IjAgMCA4cHgifQ==\",\"f_ex_font_family\":\"521\",\"ex_txt\":\"#000000\",\"show_btn\":\"none\",\"f_ex_font_size\":\"eyJhbGwiOiIxMyIsImxhbmRzY2FwZSI6IjEyIiwicGhvbmUiOiIxMiJ9\",\"mc1_el\":\"26\",\"art_excerpt\":\"eyJhbGwiOiIxNXB4IDAgMCIsImxhbmRzY2FwZSI6IjEzcHggMCAwIiwicGhvbmUiOiIxMXB4IDAgMCJ9\",\"f_ex_font_line_height\":\"eyJsYW5kc2NhcGUiOiIxLjUifQ==\",\"show_excerpt\":\"eyJwb3J0cmFpdCI6Im5vbmUiLCJhbGwiOiJub25lIn0=\",\"ajax_pagination\":\"load_more\",\"pag_space\":\"eyJhbGwiOiI0MiIsImxhbmRzY2FwZSI6IjM0IiwicG9ydHJhaXQiOiIyNCIsInBob25lIjoiMzQifQ==\",\"pag_h_bg\":\"#dd3333\",\"pag_a_bg\":\"#dd3333\",\"pag_h_border\":\"#dd3333\",\"pag_a_border\":\"#dd3333\",\"all_modules_space\":\"eyJhbGwiOiI0MiIsImxhbmRzY2FwZSI6IjM0IiwicG9ydHJhaXQiOiIyNCIsInBob25lIjoiMzQifQ==\",\"all_underline_color\":\"#002e44\",\"show_com\":\"none\",\"block_type\":\"tdb_loop\",\"separator\":\"\",\"custom_title\":\"\",\"custom_url\":\"\",\"block_template_id\":\"\",\"title_tag\":\"\",\"mc1_tl\":\"\",\"mc1_title_tag\":\"\",\"limit\":\"\",\"offset\":\"\",\"open_in_new_window\":\"\",\"post_ids\":\"-8637\",\"include_cf_posts\":\"\",\"exclude_cf_posts\":\"\",\"sort\":\"\",\"installed_post_types\":\"\",\"ajax_pagination_next_prev_swipe\":\"\",\"ajax_pagination_infinite_stop\":\"\",\"review_source\":\"\",\"container_width\":\"\",\"modules_gap\":\"48\",\"m_padding\":\"\",\"m_radius\":\"\",\"modules_border_size\":\"\",\"modules_border_style\":\"\",\"modules_border_color\":\"#eaeaea\",\"modules_divider\":\"\",\"modules_divider_color\":\"#eaeaea\",\"h_effect\":\"\",\"image_size\":\"\",\"image_alignment\":\"50\",\"hide_image\":\"\",\"show_favourites\":\"\",\"fav_size\":\"2\",\"fav_space\":\"\",\"fav_ico_color\":\"\",\"fav_ico_color_h\":\"\",\"fav_bg\":\"\",\"fav_bg_h\":\"\",\"fav_shadow_shadow_header\":\"\",\"fav_shadow_shadow_title\":\"Shadow\",\"fav_shadow_shadow_size\":\"\",\"fav_shadow_shadow_offset_horizontal\":\"\",\"fav_shadow_shadow_offset_vertical\":\"\",\"fav_shadow_shadow_spread\":\"\",\"fav_shadow_shadow_color\":\"\",\"video_icon\":\"\",\"video_popup\":\"yes\",\"video_rec\":\"\",\"spot_header\":\"\",\"video_rec_title\":\"- Advertisement -\",\"video_rec_color\":\"\",\"video_rec_disable\":\"\",\"autoplay_vid\":\"yes\",\"show_vid_t\":\"block\",\"vid_t_margin\":\"\",\"vid_t_padding\":\"\",\"video_title_color\":\"\",\"video_title_color_h\":\"\",\"video_bg\":\"\",\"video_overlay\":\"\",\"vid_t_color\":\"\",\"vid_t_bg_color\":\"\",\"f_vid_title_font_header\":\"\",\"f_vid_title_font_title\":\"Video pop-up article title\",\"f_vid_title_font_settings\":\"\",\"f_vid_title_font_family\":\"\",\"f_vid_title_font_size\":\"\",\"f_vid_title_font_line_height\":\"\",\"f_vid_title_font_style\":\"\",\"f_vid_title_font_weight\":\"\",\"f_vid_title_font_transform\":\"\",\"f_vid_title_font_spacing\":\"\",\"f_vid_title_\":\"\",\"f_vid_time_font_title\":\"Video duration text\",\"f_vid_time_font_settings\":\"\",\"f_vid_time_font_family\":\"\",\"f_vid_time_font_size\":\"\",\"f_vid_time_font_line_height\":\"\",\"f_vid_time_font_style\":\"\",\"f_vid_time_font_weight\":\"\",\"f_vid_time_font_transform\":\"\",\"f_vid_time_font_spacing\":\"\",\"f_vid_time_\":\"\",\"meta_width\":\"\",\"meta_margin\":\"\",\"meta_space\":\"\",\"meta_info_border_size\":\"\",\"meta_info_border_style\":\"\",\"meta_info_border_color\":\"#eaeaea\",\"meta_info_border_radius\":\"\",\"art_btn\":\"\",\"modules_cat_border\":\"\",\"modules_category_radius\":\"0\",\"show_cat\":\"inline-block\",\"modules_extra_cat\":\"\",\"author_photo\":\"\",\"author_photo_size\":\"\",\"author_photo_space\":\"\",\"author_photo_radius\":\"\",\"show_date\":\"inline-block\",\"show_modified_date\":\"\",\"time_ago\":\"\",\"time_ago_add_txt\":\"ago\",\"time_ago_txt_pos\":\"\",\"show_review\":\"inline-block\",\"review_space\":\"\",\"review_size\":\"2.5\",\"review_distance\":\"\",\"excerpt_col\":\"1\",\"excerpt_gap\":\"\",\"excerpt_middle\":\"\",\"excerpt_inline\":\"\",\"show_audio\":\"block\",\"art_audio\":\"\",\"art_audio_size\":\"1.5\",\"btn_title\":\"\",\"btn_margin\":\"\",\"btn_padding\":\"\",\"btn_border_width\":\"\",\"btn_radius\":\"\",\"pag_padding\":\"\",\"pag_border_width\":\"\",\"pag_border_radius\":\"\",\"prev_tdicon\":\"\",\"next_tdicon\":\"\",\"pag_icons_size\":\"\",\"f_header_font_header\":\"\",\"f_header_font_title\":\"Block header\",\"f_header_font_settings\":\"\",\"f_header_font_family\":\"\",\"f_header_font_size\":\"\",\"f_header_font_line_height\":\"\",\"f_header_font_style\":\"\",\"f_header_font_weight\":\"\",\"f_header_font_transform\":\"\",\"f_header_font_spacing\":\"\",\"f_header_\":\"\",\"f_pag_font_title\":\"Pagination text\",\"f_pag_font_settings\":\"\",\"f_pag_font_family\":\"\",\"f_pag_font_size\":\"\",\"f_pag_font_line_height\":\"\",\"f_pag_font_style\":\"\",\"f_pag_font_weight\":\"\",\"f_pag_font_transform\":\"\",\"f_pag_font_spacing\":\"\",\"f_pag_\":\"\",\"f_title_font_header\":\"\",\"f_title_font_title\":\"Article title\",\"f_title_font_settings\":\"\",\"f_title_font_style\":\"\",\"f_title_font_transform\":\"\",\"f_title_font_spacing\":\"\",\"f_title_\":\"\",\"f_cat_font_title\":\"Article category tag\",\"f_cat_font_settings\":\"\",\"f_cat_font_family\":\"\",\"f_cat_font_style\":\"\",\"f_cat_font_weight\":\"\",\"f_cat_font_transform\":\"\",\"f_cat_font_spacing\":\"\",\"f_cat_\":\"\",\"f_meta_font_title\":\"Article meta info\",\"f_meta_font_settings\":\"\",\"f_meta_font_family\":\"\",\"f_meta_font_style\":\"\",\"f_meta_font_weight\":\"\",\"f_meta_font_transform\":\"\",\"f_meta_font_spacing\":\"\",\"f_meta_\":\"\",\"f_ex_font_title\":\"Article excerpt\",\"f_ex_font_settings\":\"\",\"f_ex_font_style\":\"\",\"f_ex_font_weight\":\"\",\"f_ex_font_transform\":\"\",\"f_ex_font_spacing\":\"\",\"f_ex_\":\"\",\"f_btn_font_title\":\"Article read more button\",\"f_btn_font_settings\":\"\",\"f_btn_font_family\":\"\",\"f_btn_font_size\":\"\",\"f_btn_font_line_height\":\"\",\"f_btn_font_style\":\"\",\"f_btn_font_weight\":\"\",\"f_btn_font_transform\":\"\",\"f_btn_font_spacing\":\"\",\"f_btn_\":\"\",\"mix_color\":\"\",\"mix_type\":\"\",\"fe_brightness\":\"1\",\"fe_contrast\":\"1\",\"fe_saturate\":\"1\",\"mix_color_h\":\"\",\"mix_type_h\":\"\",\"fe_brightness_h\":\"1\",\"fe_contrast_h\":\"1\",\"fe_saturate_h\":\"1\",\"m_bg\":\"\",\"shadow_shadow_header\":\"\",\"shadow_shadow_title\":\"Module Shadow\",\"shadow_shadow_size\":\"\",\"shadow_shadow_offset_horizontal\":\"\",\"shadow_shadow_offset_vertical\":\"\",\"shadow_shadow_spread\":\"\",\"shadow_shadow_color\":\"\",\"title_txt\":\"\",\"all_underline_height\":\"\",\"cat_border\":\"\",\"cat_border_hover\":\"\",\"meta_bg\":\"\",\"author_txt\":\"\",\"author_txt_hover\":\"\",\"rev_txt\":\"\",\"shadow_m_shadow_header\":\"\",\"shadow_m_shadow_title\":\"Meta info shadow\",\"shadow_m_shadow_size\":\"\",\"shadow_m_shadow_offset_horizontal\":\"\",\"shadow_m_shadow_offset_vertical\":\"\",\"shadow_m_shadow_spread\":\"\",\"shadow_m_shadow_color\":\"\",\"audio_btn_color\":\"\",\"audio_time_color\":\"\",\"audio_bar_color\":\"\",\"audio_bar_curr_color\":\"\",\"btn_bg\":\"\",\"btn_bg_hover\":\"\",\"btn_txt\":\"\",\"btn_txt_hover\":\"\",\"btn_border\":\"\",\"btn_border_hover\":\"\",\"nextprev_border_h\":\"\",\"pag_text\":\"\",\"pag_h_text\":\"\",\"pag_a_text\":\"\",\"pag_bg\":\"\",\"pag_border\":\"\",\"ad_loop\":\"\",\"ad_loop_title\":\"- Advertisement -\",\"ad_loop_repeat\":\"\",\"ad_loop_color\":\"\",\"ad_loop_full\":\"yes\",\"f_ad_font_header\":\"\",\"f_ad_font_title\":\"Ad title text\",\"f_ad_font_settings\":\"\",\"f_ad_font_family\":\"\",\"f_ad_font_size\":\"\",\"f_ad_font_line_height\":\"\",\"f_ad_font_style\":\"\",\"f_ad_font_weight\":\"\",\"f_ad_font_transform\":\"\",\"f_ad_font_spacing\":\"\",\"f_ad_\":\"\",\"ad_loop_disable\":\"\",\"el_class\":\"\",\"td_column_number\":2,\"header_color\":\"\",\"td_ajax_preloading\":\"\",\"td_ajax_filter_type\":\"\",\"td_filter_default_txt\":\"\",\"td_ajax_filter_ids\":\"\",\"color_preset\":\"\",\"border_top\":\"\",\"css\":\"\",\"class\":\"tdi_15\",\"tdc_css_class\":\"tdi_15\",\"tdc_css_class_style\":\"tdi_15_rand_style\"}}",
                "td_block_id": "tdi_15",
                "td_column_number": "2",
                "td_current_page": "1",
                "block_type": "tdb_loop",
                "td_filter_value": "",
                "td_user_action": "",
                "td_magic_token": "f12228ab68"
            }
            yield feapder.Request(url, data=data, params=params, callback=self.parse_url, page=page, method='POST',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://euronews.al",
            "priority": "u=1, i",
            "referer": "https://euronews.al/en/?s=heavy+rain",
            "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Microsoft Edge\";v=\"133\", \"Chromium\";v=\"133\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
            "x-requested-with": "XMLHttpRequest"
        }
        request.headers['cookie'] = '_gid=GA1.2.1452300808.1739946051; _fbp=fb.1.1739946050937.919047976636015923; cf_clearance=xW74FOIB.IXt8hNlPXvn9puuNP.k4FG7Ycc6YtV.G0M-1740019344-1.2.1.1-E6.y1HSTN9ulNLDHc4VRwrOEomhWTY52wuam22eQhUx9dW769aC.ITJMvaEuicI5k6BH53XkJz8UGdRtd4ptMF3x59s70w2TQMhW9cDUpqRlJisc6PNkZLVjaVwGCMGsfqF5cpO_SugZOmAvAc3ee3EPZosdVomEPwfi0JkIEjNPPloATyC31hBNhtIAB5QW49Q4LwawAYQXjhQgevqk7QU6sJiO77nToIqIXm2YDFry01tOogtMXyJYFLhqcsw8q4gJmvm1phdkBa0AZj0ZLi58VkHdoPV4M2SAbVltpuM; _ga_NN6N6FGGLJ=GS1.1.1740019361.2.0.1740019516.0.0.0; _ga_BH4Z96ERK8=GS1.1.1740019361.3.1.1740019541.0.0.0; _ga=GA1.2.165605768.1739946051'
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        links = etree.HTML(response.json['td_data']).xpath("//div[@class='td-module-thumb']/a/@href")

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
            items.table_name = self.table  # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            # items.title = item[0].get("title")
            items.article_url = item
            # items.pubtime = item[0].get("public_date")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://euronews.al/en/wp-admin/admin-ajax.php"
        params = {
            "td_theme_name": "Newspaper",
            "v": "12.6.6"
        }
        data = {
            "action": "td_ajax_block",
            "td_atts": f"{{\"search_query\":\"{current_keyword}\",\"modules_on_row\":\"\",\"hide_audio\":\"yes\",\"category_id\":\"\",\"image_width\":\"eyJhbGwiOiI0NyIsInBvcnRyYWl0IjoiNDAiLCJwaG9uZSI6IjEwMCJ9\",\"image_floated\":\"eyJhbGwiOiJmbG9hdF9sZWZ0IiwicGhvbmUiOiJub19mbG9hdCJ9\",\"image_radius\":\"\",\"image_height\":\"eyJhbGwiOiI2NSIsImxhbmRzY2FwZSI6IjcwIiwicG9ydHJhaXQiOiI3MCIsInBob25lIjoiNjAifQ==\",\"meta_info_horiz\":\"\",\"modules_category\":\"\",\"modules_category_margin\":\"eyJhbGwiOiIycHggMTBweCAwIDAiLCJsYW5kc2NhcGUiOiIycHggOHB4IDAgMCIsInBvcnRyYWl0IjoiMnB4IDhweCAwIDAiLCJwaG9uZSI6IjJweCA4cHggMCAwIn0=\",\"show_author\":\"none\",\"tdc_css\":\"eyJhbGwiOnsibWFyZ2luLWJvdHRvbSI6IjAiLCJkaXNwbGF5IjoiIn0sImxhbmRzY2FwZSI6eyJkaXNwbGF5IjoiIn0sImxhbmRzY2FwZV9tYXhfd2lkdGgiOjExNDAsImxhbmRzY2FwZV9taW5fd2lkdGgiOjEwMTksInBvcnRyYWl0Ijp7ImRpc3BsYXkiOiIifSwicG9ydHJhaXRfbWF4X3dpZHRoIjoxMDE4LCJwb3J0cmFpdF9taW5fd2lkdGgiOjc2OCwicGhvbmUiOnsiZGlzcGxheSI6IiJ9LCJwaG9uZV9tYXhfd2lkdGgiOjc2N30=\",\"meta_padding\":\"eyJhbGwiOiIwIDAgMCA0MnB4IiwibGFuZHNjYXBlIjoiMCAwIDAgMzRweCIsInBvcnRyYWl0IjoiMCAwIDAgMjRweCIsInBob25lIjoiMThweCAwIDAifQ==\",\"meta_info_align\":\"center\",\"f_title_font_family\":\"522\",\"f_title_font_weight\":\"700\",\"f_title_font_size\":\"eyJhbGwiOiIyOCIsImxhbmRzY2FwZSI6IjI0IiwicG9ydHJhaXQiOiIyMiIsInBob25lIjoiMjQifQ==\",\"f_title_font_line_height\":\"1.2\",\"title_txt_hover\":\"#017df4\",\"cat_bg\":\"rgba(255,53,53,0)\",\"cat_bg_hover\":\"rgba(255,53,53,0)\",\"cat_txt\":\"#002e44\",\"date_txt\":\"#727277\",\"f_meta_font_size\":\"eyJhbGwiOiIxMyIsImxhbmRzY2FwZSI6IjEyIiwicG9ydHJhaXQiOiIxMiIsInBob25lIjoiMTIifQ==\",\"f_cat_font_size\":\"eyJhbGwiOiIxMyIsImxhbmRzY2FwZSI6IjEyIiwicG9ydHJhaXQiOiIxMiIsInBob25lIjoiMTIifQ==\",\"f_cat_font_line_height\":\"1\",\"f_meta_font_line_height\":\"1\",\"modules_category_padding\":\"0\",\"cat_txt_hover\":\"#000000\",\"com_bg\":\"#eaeaea\",\"com_txt\":\"#727277\",\"art_title\":\"eyJhbGwiOiIwIDAgMTBweCIsImxhbmRzY2FwZSI6IjAgMCA4cHgiLCJwaG9uZSI6IjAgMCA4cHgifQ==\",\"f_ex_font_family\":\"521\",\"ex_txt\":\"#000000\",\"show_btn\":\"none\",\"f_ex_font_size\":\"eyJhbGwiOiIxMyIsImxhbmRzY2FwZSI6IjEyIiwicGhvbmUiOiIxMiJ9\",\"mc1_el\":\"26\",\"art_excerpt\":\"eyJhbGwiOiIxNXB4IDAgMCIsImxhbmRzY2FwZSI6IjEzcHggMCAwIiwicGhvbmUiOiIxMXB4IDAgMCJ9\",\"f_ex_font_line_height\":\"eyJsYW5kc2NhcGUiOiIxLjUifQ==\",\"show_excerpt\":\"eyJwb3J0cmFpdCI6Im5vbmUiLCJhbGwiOiJub25lIn0=\",\"ajax_pagination\":\"load_more\",\"pag_space\":\"eyJhbGwiOiI0MiIsImxhbmRzY2FwZSI6IjM0IiwicG9ydHJhaXQiOiIyNCIsInBob25lIjoiMzQifQ==\",\"pag_h_bg\":\"#dd3333\",\"pag_a_bg\":\"#dd3333\",\"pag_h_border\":\"#dd3333\",\"pag_a_border\":\"#dd3333\",\"all_modules_space\":\"eyJhbGwiOiI0MiIsImxhbmRzY2FwZSI6IjM0IiwicG9ydHJhaXQiOiIyNCIsInBob25lIjoiMzQifQ==\",\"all_underline_color\":\"#002e44\",\"show_com\":\"none\",\"block_type\":\"tdb_loop\",\"separator\":\"\",\"custom_title\":\"\",\"custom_url\":\"\",\"block_template_id\":\"\",\"title_tag\":\"\",\"mc1_tl\":\"\",\"mc1_title_tag\":\"\",\"limit\":\"\",\"offset\":\"\",\"open_in_new_window\":\"\",\"post_ids\":\"-8637\",\"include_cf_posts\":\"\",\"exclude_cf_posts\":\"\",\"sort\":\"\",\"installed_post_types\":\"\",\"ajax_pagination_next_prev_swipe\":\"\",\"ajax_pagination_infinite_stop\":\"\",\"review_source\":\"\",\"container_width\":\"\",\"modules_gap\":\"48\",\"m_padding\":\"\",\"m_radius\":\"\",\"modules_border_size\":\"\",\"modules_border_style\":\"\",\"modules_border_color\":\"#eaeaea\",\"modules_divider\":\"\",\"modules_divider_color\":\"#eaeaea\",\"h_effect\":\"\",\"image_size\":\"\",\"image_alignment\":\"50\",\"hide_image\":\"\",\"show_favourites\":\"\",\"fav_size\":\"2\",\"fav_space\":\"\",\"fav_ico_color\":\"\",\"fav_ico_color_h\":\"\",\"fav_bg\":\"\",\"fav_bg_h\":\"\",\"fav_shadow_shadow_header\":\"\",\"fav_shadow_shadow_title\":\"Shadow\",\"fav_shadow_shadow_size\":\"\",\"fav_shadow_shadow_offset_horizontal\":\"\",\"fav_shadow_shadow_offset_vertical\":\"\",\"fav_shadow_shadow_spread\":\"\",\"fav_shadow_shadow_color\":\"\",\"video_icon\":\"\",\"video_popup\":\"yes\",\"video_rec\":\"\",\"spot_header\":\"\",\"video_rec_title\":\"- Advertisement -\",\"video_rec_color\":\"\",\"video_rec_disable\":\"\",\"autoplay_vid\":\"yes\",\"show_vid_t\":\"block\",\"vid_t_margin\":\"\",\"vid_t_padding\":\"\",\"video_title_color\":\"\",\"video_title_color_h\":\"\",\"video_bg\":\"\",\"video_overlay\":\"\",\"vid_t_color\":\"\",\"vid_t_bg_color\":\"\",\"f_vid_title_font_header\":\"\",\"f_vid_title_font_title\":\"Video pop-up article title\",\"f_vid_title_font_settings\":\"\",\"f_vid_title_font_family\":\"\",\"f_vid_title_font_size\":\"\",\"f_vid_title_font_line_height\":\"\",\"f_vid_title_font_style\":\"\",\"f_vid_title_font_weight\":\"\",\"f_vid_title_font_transform\":\"\",\"f_vid_title_font_spacing\":\"\",\"f_vid_title_\":\"\",\"f_vid_time_font_title\":\"Video duration text\",\"f_vid_time_font_settings\":\"\",\"f_vid_time_font_family\":\"\",\"f_vid_time_font_size\":\"\",\"f_vid_time_font_line_height\":\"\",\"f_vid_time_font_style\":\"\",\"f_vid_time_font_weight\":\"\",\"f_vid_time_font_transform\":\"\",\"f_vid_time_font_spacing\":\"\",\"f_vid_time_\":\"\",\"meta_width\":\"\",\"meta_margin\":\"\",\"meta_space\":\"\",\"meta_info_border_size\":\"\",\"meta_info_border_style\":\"\",\"meta_info_border_color\":\"#eaeaea\",\"meta_info_border_radius\":\"\",\"art_btn\":\"\",\"modules_cat_border\":\"\",\"modules_category_radius\":\"0\",\"show_cat\":\"inline-block\",\"modules_extra_cat\":\"\",\"author_photo\":\"\",\"author_photo_size\":\"\",\"author_photo_space\":\"\",\"author_photo_radius\":\"\",\"show_date\":\"inline-block\",\"show_modified_date\":\"\",\"time_ago\":\"\",\"time_ago_add_txt\":\"ago\",\"time_ago_txt_pos\":\"\",\"show_review\":\"inline-block\",\"review_space\":\"\",\"review_size\":\"2.5\",\"review_distance\":\"\",\"excerpt_col\":\"1\",\"excerpt_gap\":\"\",\"excerpt_middle\":\"\",\"excerpt_inline\":\"\",\"show_audio\":\"block\",\"art_audio\":\"\",\"art_audio_size\":\"1.5\",\"btn_title\":\"\",\"btn_margin\":\"\",\"btn_padding\":\"\",\"btn_border_width\":\"\",\"btn_radius\":\"\",\"pag_padding\":\"\",\"pag_border_width\":\"\",\"pag_border_radius\":\"\",\"prev_tdicon\":\"\",\"next_tdicon\":\"\",\"pag_icons_size\":\"\",\"f_header_font_header\":\"\",\"f_header_font_title\":\"Block header\",\"f_header_font_settings\":\"\",\"f_header_font_family\":\"\",\"f_header_font_size\":\"\",\"f_header_font_line_height\":\"\",\"f_header_font_style\":\"\",\"f_header_font_weight\":\"\",\"f_header_font_transform\":\"\",\"f_header_font_spacing\":\"\",\"f_header_\":\"\",\"f_pag_font_title\":\"Pagination text\",\"f_pag_font_settings\":\"\",\"f_pag_font_family\":\"\",\"f_pag_font_size\":\"\",\"f_pag_font_line_height\":\"\",\"f_pag_font_style\":\"\",\"f_pag_font_weight\":\"\",\"f_pag_font_transform\":\"\",\"f_pag_font_spacing\":\"\",\"f_pag_\":\"\",\"f_title_font_header\":\"\",\"f_title_font_title\":\"Article title\",\"f_title_font_settings\":\"\",\"f_title_font_style\":\"\",\"f_title_font_transform\":\"\",\"f_title_font_spacing\":\"\",\"f_title_\":\"\",\"f_cat_font_title\":\"Article category tag\",\"f_cat_font_settings\":\"\",\"f_cat_font_family\":\"\",\"f_cat_font_style\":\"\",\"f_cat_font_weight\":\"\",\"f_cat_font_transform\":\"\",\"f_cat_font_spacing\":\"\",\"f_cat_\":\"\",\"f_meta_font_title\":\"Article meta info\",\"f_meta_font_settings\":\"\",\"f_meta_font_family\":\"\",\"f_meta_font_style\":\"\",\"f_meta_font_weight\":\"\",\"f_meta_font_transform\":\"\",\"f_meta_font_spacing\":\"\",\"f_meta_\":\"\",\"f_ex_font_title\":\"Article excerpt\",\"f_ex_font_settings\":\"\",\"f_ex_font_style\":\"\",\"f_ex_font_weight\":\"\",\"f_ex_font_transform\":\"\",\"f_ex_font_spacing\":\"\",\"f_ex_\":\"\",\"f_btn_font_title\":\"Article read more button\",\"f_btn_font_settings\":\"\",\"f_btn_font_family\":\"\",\"f_btn_font_size\":\"\",\"f_btn_font_line_height\":\"\",\"f_btn_font_style\":\"\",\"f_btn_font_weight\":\"\",\"f_btn_font_transform\":\"\",\"f_btn_font_spacing\":\"\",\"f_btn_\":\"\",\"mix_color\":\"\",\"mix_type\":\"\",\"fe_brightness\":\"1\",\"fe_contrast\":\"1\",\"fe_saturate\":\"1\",\"mix_color_h\":\"\",\"mix_type_h\":\"\",\"fe_brightness_h\":\"1\",\"fe_contrast_h\":\"1\",\"fe_saturate_h\":\"1\",\"m_bg\":\"\",\"shadow_shadow_header\":\"\",\"shadow_shadow_title\":\"Module Shadow\",\"shadow_shadow_size\":\"\",\"shadow_shadow_offset_horizontal\":\"\",\"shadow_shadow_offset_vertical\":\"\",\"shadow_shadow_spread\":\"\",\"shadow_shadow_color\":\"\",\"title_txt\":\"\",\"all_underline_height\":\"\",\"cat_border\":\"\",\"cat_border_hover\":\"\",\"meta_bg\":\"\",\"author_txt\":\"\",\"author_txt_hover\":\"\",\"rev_txt\":\"\",\"shadow_m_shadow_header\":\"\",\"shadow_m_shadow_title\":\"Meta info shadow\",\"shadow_m_shadow_size\":\"\",\"shadow_m_shadow_offset_horizontal\":\"\",\"shadow_m_shadow_offset_vertical\":\"\",\"shadow_m_shadow_spread\":\"\",\"shadow_m_shadow_color\":\"\",\"audio_btn_color\":\"\",\"audio_time_color\":\"\",\"audio_bar_color\":\"\",\"audio_bar_curr_color\":\"\",\"btn_bg\":\"\",\"btn_bg_hover\":\"\",\"btn_txt\":\"\",\"btn_txt_hover\":\"\",\"btn_border\":\"\",\"btn_border_hover\":\"\",\"nextprev_border_h\":\"\",\"pag_text\":\"\",\"pag_h_text\":\"\",\"pag_a_text\":\"\",\"pag_bg\":\"\",\"pag_border\":\"\",\"ad_loop\":\"\",\"ad_loop_title\":\"- Advertisement -\",\"ad_loop_repeat\":\"\",\"ad_loop_color\":\"\",\"ad_loop_full\":\"yes\",\"f_ad_font_header\":\"\",\"f_ad_font_title\":\"Ad title text\",\"f_ad_font_settings\":\"\",\"f_ad_font_family\":\"\",\"f_ad_font_size\":\"\",\"f_ad_font_line_height\":\"\",\"f_ad_font_style\":\"\",\"f_ad_font_weight\":\"\",\"f_ad_font_transform\":\"\",\"f_ad_font_spacing\":\"\",\"f_ad_\":\"\",\"ad_loop_disable\":\"\",\"el_class\":\"\",\"td_column_number\":2,\"header_color\":\"\",\"td_ajax_preloading\":\"\",\"td_ajax_filter_type\":\"\",\"td_filter_default_txt\":\"\",\"td_ajax_filter_ids\":\"\",\"color_preset\":\"\",\"border_top\":\"\",\"css\":\"\",\"class\":\"tdi_15\",\"tdc_css_class\":\"tdi_15\",\"tdc_css_class_style\":\"tdi_15_rand_style\"}}",
            "td_block_id": "tdi_15",
            "td_column_number": "2",
            "td_current_page": f"{current_page}",
            "block_type": "tdb_loop",
            "td_filter_value": "",
            "td_user_action": "",
            "td_magic_token": "6e2aad0d03"
        }
        yield feapder.Request(url, data=data, params=params, callback=self.parse_url, page=current_page, method='POST',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//meta[@property='og:title']/text()").extract_first()
        items.content = "".join(
            response.xpath(
                "//div[@class='tdb-block-inner td-fix-index']//p//text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        # if items.content:
        #     yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
