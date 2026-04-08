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

    country = 'Brunei Darussalam'
    table = 'BruneiDarussalam_borneobulletin'
    keywords = ["heatwave","Extreme heatwave", "High temperature", "Extreme temperature", "Heatwave event", "Increase in high temperature", "High temperature impact", "Strong heat", "Temperature rise", "Heat event", "Temperature increase", "Heavy rainfall", "Heavy precipitation", "Torrential rain", "Extreme rainfall", "Drought", "Severe drought", "Long-term drought", "Water shortage", "Power outage", "High temperature power outage", "Heatwave power outage", "Power outage caused by high temperature", "Fire", "High temperature fire", "Heat fire", "Temperature fire", "Heat-induced fire", "Agricultural impact", "Heatwave agriculture", "Crop damage", "Agricultural heat stress", "Hypoxia", "Heatstroke", "Heat-induced heatstroke", "High temperature hypoxia", "High temperature heatstroke", "Traffic impact", "High temperature traffic", "Heatwave traffic", "Temperature traffic", "Ecological disaster", "Heat disaster", "High temperature environment", "Impact of heat on biodiversity", "Heatwave ecology", "Pollution", "High temperature pollution", "Heat pollution", "Temperature pollution", "Coral bleaching", "High temperature coral reefs", "Temperature coral bleaching" ]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://borneobulletin.com.bn/wp-admin/admin-ajax.php"
            params = {
                "td_theme_name": "Newspaper",
                "v": "12.6.8"
            }
            data = {
                "action": "td_ajax_block",
                "td_atts": f"{{\"modules_on_row\":\"\",\"limit\":\"10\",\"hide_audio\":\"yes\",\"category_id\":\"_current_search\",\"image_floated\":\"float_left\",\"modules_category\":\"above\",\"modules_extra_cat\":\"hide\",\"show_author\":\"none\",\"show_btn\":\"none\",\"show_com\":\"none\",\"show_review\":\"none\",\"show_excerpt\":\"\",\"ajax_pagination\":\"infinite\",\"td_ajax_preloading\":\"preload_all\",\"image_height\":\"60\",\"image_width\":\"40\",\"art_excerpt\":\"10px 0px\",\"meta_margin\":\"0px 10px\",\"meta_padding\":\"0px\",\"art_title\":\"0px 0px 10px 0px\",\"modules_category_margin\":\"0px 0px 10px 0px\",\"f_title_font_weight\":\"600\",\"f_title_font_family\":\"721\",\"f_title_font_transform\":\"none\",\"f_meta_font_size\":\"13\",\"f_meta_font_family\":\"582\",\"f_meta_font_weight\":\"400\",\"f_ex_font_family\":\"582\",\"f_ex_font_size\":\"13\",\"f_title_font_size\":\"22\",\"block_type\":\"td_flex_block_1\",\"separator\":\"\",\"custom_title\":\"\",\"custom_url\":\"\",\"block_template_id\":\"\",\"title_tag\":\"\",\"mc1_tl\":\"\",\"mc1_title_tag\":\"\",\"mc1_el\":\"\",\"post_ids\":\"-557052\",\"taxonomies\":\"\",\"category_ids\":\"\",\"in_all_terms\":\"\",\"tag_slug\":\"\",\"autors_id\":\"\",\"installed_post_types\":\"\",\"include_cf_posts\":\"\",\"exclude_cf_posts\":\"\",\"sort\":\"\",\"popular_by_date\":\"\",\"linked_posts\":\"\",\"favourite_only\":\"\",\"offset\":\"\",\"open_in_new_window\":\"\",\"show_modified_date\":\"\",\"time_ago\":\"\",\"time_ago_add_txt\":\"ago\",\"time_ago_txt_pos\":\"\",\"review_source\":\"\",\"el_class\":\"\",\"td_query_cache\":\"\",\"td_query_cache_expiration\":\"\",\"td_ajax_filter_type\":\"\",\"td_ajax_filter_ids\":\"\",\"td_filter_default_txt\":\"All\",\"container_width\":\"\",\"modules_gap\":\"\",\"m_padding\":\"\",\"all_modules_space\":\"36\",\"modules_border_size\":\"\",\"modules_border_style\":\"\",\"modules_border_color\":\"#eaeaea\",\"modules_border_radius\":\"\",\"modules_divider\":\"\",\"modules_divider_color\":\"#eaeaea\",\"h_effect\":\"\",\"image_size\":\"\",\"image_alignment\":\"50\",\"image_radius\":\"\",\"hide_image\":\"\",\"show_favourites\":\"\",\"fav_size\":\"2\",\"fav_space\":\"\",\"fav_ico_color\":\"\",\"fav_ico_color_h\":\"\",\"fav_bg\":\"\",\"fav_bg_h\":\"\",\"fav_shadow_shadow_header\":\"\",\"fav_shadow_shadow_title\":\"Shadow\",\"fav_shadow_shadow_size\":\"\",\"fav_shadow_shadow_offset_horizontal\":\"\",\"fav_shadow_shadow_offset_vertical\":\"\",\"fav_shadow_shadow_spread\":\"\",\"fav_shadow_shadow_color\":\"\",\"video_icon\":\"\",\"video_popup\":\"yes\",\"video_rec\":\"\",\"spot_header\":\"\",\"video_rec_title\":\"\",\"video_rec_color\":\"\",\"video_rec_disable\":\"\",\"autoplay_vid\":\"yes\",\"show_vid_t\":\"block\",\"vid_t_margin\":\"\",\"vid_t_padding\":\"\",\"video_title_color\":\"\",\"video_title_color_h\":\"\",\"video_bg\":\"\",\"video_overlay\":\"\",\"vid_t_color\":\"\",\"vid_t_bg_color\":\"\",\"f_vid_title_font_header\":\"\",\"f_vid_title_font_title\":\"Video pop-up article title\",\"f_vid_title_font_settings\":\"\",\"f_vid_title_font_family\":\"\",\"f_vid_title_font_size\":\"\",\"f_vid_title_font_line_height\":\"\",\"f_vid_title_font_style\":\"\",\"f_vid_title_font_weight\":\"\",\"f_vid_title_font_transform\":\"\",\"f_vid_title_font_spacing\":\"\",\"f_vid_title_\":\"\",\"f_vid_time_font_title\":\"Video duration text\",\"f_vid_time_font_settings\":\"\",\"f_vid_time_font_family\":\"\",\"f_vid_time_font_size\":\"\",\"f_vid_time_font_line_height\":\"\",\"f_vid_time_font_style\":\"\",\"f_vid_time_font_weight\":\"\",\"f_vid_time_font_transform\":\"\",\"f_vid_time_font_spacing\":\"\",\"f_vid_time_\":\"\",\"meta_info_align\":\"\",\"meta_info_horiz\":\"layout-default\",\"meta_width\":\"\",\"meta_space\":\"\",\"art_btn\":\"\",\"meta_info_border_size\":\"\",\"meta_info_border_style\":\"\",\"meta_info_border_color\":\"#eaeaea\",\"meta_info_border_radius\":\"\",\"modules_category_padding\":\"\",\"modules_cat_border\":\"\",\"modules_category_radius\":\"0\",\"show_cat\":\"inline-block\",\"author_photo\":\"\",\"author_photo_size\":\"\",\"author_photo_space\":\"\",\"author_photo_radius\":\"\",\"show_date\":\"inline-block\",\"review_space\":\"\",\"review_size\":\"2.5\",\"review_distance\":\"\",\"excerpt_col\":\"1\",\"excerpt_gap\":\"\",\"excerpt_middle\":\"\",\"excerpt_inline\":\"\",\"show_audio\":\"block\",\"art_audio\":\"\",\"art_audio_size\":\"1.5\",\"btn_title\":\"\",\"btn_margin\":\"\",\"btn_padding\":\"\",\"btn_border_width\":\"\",\"btn_radius\":\"\",\"pag_space\":\"\",\"pag_padding\":\"\",\"pag_border_width\":\"\",\"pag_border_radius\":\"\",\"prev_tdicon\":\"\",\"next_tdicon\":\"\",\"pag_icons_size\":\"\",\"f_header_font_header\":\"\",\"f_header_font_title\":\"Block header\",\"f_header_font_settings\":\"\",\"f_header_font_family\":\"\",\"f_header_font_size\":\"\",\"f_header_font_line_height\":\"\",\"f_header_font_style\":\"\",\"f_header_font_weight\":\"\",\"f_header_font_transform\":\"\",\"f_header_font_spacing\":\"\",\"f_header_\":\"\",\"f_ajax_font_title\":\"Ajax categories\",\"f_ajax_font_settings\":\"\",\"f_ajax_font_family\":\"\",\"f_ajax_font_size\":\"\",\"f_ajax_font_line_height\":\"\",\"f_ajax_font_style\":\"\",\"f_ajax_font_weight\":\"\",\"f_ajax_font_transform\":\"\",\"f_ajax_font_spacing\":\"\",\"f_ajax_\":\"\",\"f_more_font_title\":\"Load more button\",\"f_more_font_settings\":\"\",\"f_more_font_family\":\"\",\"f_more_font_size\":\"\",\"f_more_font_line_height\":\"\",\"f_more_font_style\":\"\",\"f_more_font_weight\":\"\",\"f_more_font_transform\":\"\",\"f_more_font_spacing\":\"\",\"f_more_\":\"\",\"f_title_font_header\":\"\",\"f_title_font_title\":\"Article title\",\"f_title_font_settings\":\"\",\"f_title_font_line_height\":\"\",\"f_title_font_style\":\"\",\"f_title_font_spacing\":\"\",\"f_title_\":\"\",\"f_cat_font_title\":\"Article category tag\",\"f_cat_font_settings\":\"\",\"f_cat_font_family\":\"\",\"f_cat_font_size\":\"\",\"f_cat_font_line_height\":\"\",\"f_cat_font_style\":\"\",\"f_cat_font_weight\":\"\",\"f_cat_font_transform\":\"\",\"f_cat_font_spacing\":\"\",\"f_cat_\":\"\",\"f_meta_font_title\":\"Article meta info\",\"f_meta_font_settings\":\"\",\"f_meta_font_line_height\":\"\",\"f_meta_font_style\":\"\",\"f_meta_font_transform\":\"\",\"f_meta_font_spacing\":\"\",\"f_meta_\":\"\",\"f_ex_font_title\":\"Article excerpt\",\"f_ex_font_settings\":\"\",\"f_ex_font_line_height\":\"\",\"f_ex_font_style\":\"\",\"f_ex_font_weight\":\"\",\"f_ex_font_transform\":\"\",\"f_ex_font_spacing\":\"\",\"f_ex_\":\"\",\"f_btn_font_title\":\"Article read more button\",\"f_btn_font_settings\":\"\",\"f_btn_font_family\":\"\",\"f_btn_font_size\":\"\",\"f_btn_font_line_height\":\"\",\"f_btn_font_style\":\"\",\"f_btn_font_weight\":\"\",\"f_btn_font_transform\":\"\",\"f_btn_font_spacing\":\"\",\"f_btn_\":\"\",\"mix_color\":\"\",\"mix_type\":\"\",\"fe_brightness\":\"1\",\"fe_contrast\":\"1\",\"fe_saturate\":\"1\",\"mix_color_h\":\"\",\"mix_type_h\":\"\",\"fe_brightness_h\":\"1\",\"fe_contrast_h\":\"1\",\"fe_saturate_h\":\"1\",\"m_bg\":\"\",\"color_overlay\":\"\",\"shadow_shadow_header\":\"\",\"shadow_shadow_title\":\"Module Shadow\",\"shadow_shadow_size\":\"\",\"shadow_shadow_offset_horizontal\":\"\",\"shadow_shadow_offset_vertical\":\"\",\"shadow_shadow_spread\":\"\",\"shadow_shadow_color\":\"\",\"title_txt\":\"\",\"title_txt_hover\":\"\",\"all_underline_height\":\"\",\"all_underline_color\":\"\",\"cat_style\":\"\",\"cat_bg\":\"\",\"cat_bg_hover\":\"\",\"cat_txt\":\"\",\"cat_txt_hover\":\"\",\"cat_border\":\"\",\"cat_border_hover\":\"\",\"meta_bg\":\"\",\"author_txt\":\"\",\"author_txt_hover\":\"\",\"date_txt\":\"\",\"ex_txt\":\"\",\"com_bg\":\"\",\"com_txt\":\"\",\"rev_txt\":\"\",\"audio_btn_color\":\"\",\"audio_time_color\":\"\",\"audio_bar_color\":\"\",\"audio_bar_curr_color\":\"\",\"shadow_m_shadow_header\":\"\",\"shadow_m_shadow_title\":\"Meta info shadow\",\"shadow_m_shadow_size\":\"\",\"shadow_m_shadow_offset_horizontal\":\"\",\"shadow_m_shadow_offset_vertical\":\"\",\"shadow_m_shadow_spread\":\"\",\"shadow_m_shadow_color\":\"\",\"btn_bg\":\"\",\"btn_bg_hover\":\"\",\"btn_txt\":\"\",\"btn_txt_hover\":\"\",\"btn_border\":\"\",\"btn_border_hover\":\"\",\"pag_text\":\"\",\"pag_h_text\":\"\",\"pag_bg\":\"\",\"pag_h_bg\":\"\",\"pag_border\":\"\",\"pag_h_border\":\"\",\"ajax_pagination_next_prev_swipe\":\"\",\"ajax_pagination_infinite_stop\":\"\",\"css\":\"\",\"tdc_css\":\"\",\"td_column_number\":2,\"header_color\":\"\",\"color_preset\":\"\",\"border_top\":\"\",\"class\":\"tdi_77\",\"tdc_css_class\":\"tdi_77\",\"tdc_css_class_style\":\"tdi_77_rand_style\",\"search_query\":\"{keyword}\"}}",
                "td_block_id": "tdi_77",
                "td_column_number": "2",
                "td_current_page": "1",
                "block_type": "td_flex_block_1",
                "td_filter_value": "",
                "td_user_action": "",
                "td_magic_token": "4a5ab8f3f5"
            }
            yield feapder.Request(url, params=params, data=data, callback=self.parse_url, page=page, keyword=keyword,method="POST",
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://borneobulletin.com.bn",
            "priority": "u=1, i",
            "referer": "https://borneobulletin.com.bn/?s=heavy+rain",
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
            "PHPSESSID": "3hp71gsmqtthcf3pvscsrjdbs0",
            "_ga": "GA1.1.1515404936.1737355116",
            "_ga_NK2L6R3W5K": "GS1.1.1737355115.1.1.1737355245.59.0.0"
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
        url = "https://borneobulletin.com.bn/wp-admin/admin-ajax.php"
        params = {
            "td_theme_name": "Newspaper",
            "v": "12.6.8"
        }
        data = {
            "action": "td_ajax_block",
            "td_atts": f"{{\"modules_on_row\":\"\",\"limit\":\"10\",\"hide_audio\":\"yes\",\"category_id\":\"_current_search\",\"image_floated\":\"float_left\",\"modules_category\":\"above\",\"modules_extra_cat\":\"hide\",\"show_author\":\"none\",\"show_btn\":\"none\",\"show_com\":\"none\",\"show_review\":\"none\",\"show_excerpt\":\"\",\"ajax_pagination\":\"infinite\",\"td_ajax_preloading\":\"preload_all\",\"image_height\":\"60\",\"image_width\":\"40\",\"art_excerpt\":\"10px 0px\",\"meta_margin\":\"0px 10px\",\"meta_padding\":\"0px\",\"art_title\":\"0px 0px 10px 0px\",\"modules_category_margin\":\"0px 0px 10px 0px\",\"f_title_font_weight\":\"600\",\"f_title_font_family\":\"721\",\"f_title_font_transform\":\"none\",\"f_meta_font_size\":\"13\",\"f_meta_font_family\":\"582\",\"f_meta_font_weight\":\"400\",\"f_ex_font_family\":\"582\",\"f_ex_font_size\":\"13\",\"f_title_font_size\":\"22\",\"block_type\":\"td_flex_block_1\",\"separator\":\"\",\"custom_title\":\"\",\"custom_url\":\"\",\"block_template_id\":\"\",\"title_tag\":\"\",\"mc1_tl\":\"\",\"mc1_title_tag\":\"\",\"mc1_el\":\"\",\"post_ids\":\"-557052\",\"taxonomies\":\"\",\"category_ids\":\"\",\"in_all_terms\":\"\",\"tag_slug\":\"\",\"autors_id\":\"\",\"installed_post_types\":\"\",\"include_cf_posts\":\"\",\"exclude_cf_posts\":\"\",\"sort\":\"\",\"popular_by_date\":\"\",\"linked_posts\":\"\",\"favourite_only\":\"\",\"offset\":\"\",\"open_in_new_window\":\"\",\"show_modified_date\":\"\",\"time_ago\":\"\",\"time_ago_add_txt\":\"ago\",\"time_ago_txt_pos\":\"\",\"review_source\":\"\",\"el_class\":\"\",\"td_query_cache\":\"\",\"td_query_cache_expiration\":\"\",\"td_ajax_filter_type\":\"\",\"td_ajax_filter_ids\":\"\",\"td_filter_default_txt\":\"All\",\"container_width\":\"\",\"modules_gap\":\"\",\"m_padding\":\"\",\"all_modules_space\":\"36\",\"modules_border_size\":\"\",\"modules_border_style\":\"\",\"modules_border_color\":\"#eaeaea\",\"modules_border_radius\":\"\",\"modules_divider\":\"\",\"modules_divider_color\":\"#eaeaea\",\"h_effect\":\"\",\"image_size\":\"\",\"image_alignment\":\"50\",\"image_radius\":\"\",\"hide_image\":\"\",\"show_favourites\":\"\",\"fav_size\":\"2\",\"fav_space\":\"\",\"fav_ico_color\":\"\",\"fav_ico_color_h\":\"\",\"fav_bg\":\"\",\"fav_bg_h\":\"\",\"fav_shadow_shadow_header\":\"\",\"fav_shadow_shadow_title\":\"Shadow\",\"fav_shadow_shadow_size\":\"\",\"fav_shadow_shadow_offset_horizontal\":\"\",\"fav_shadow_shadow_offset_vertical\":\"\",\"fav_shadow_shadow_spread\":\"\",\"fav_shadow_shadow_color\":\"\",\"video_icon\":\"\",\"video_popup\":\"yes\",\"video_rec\":\"\",\"spot_header\":\"\",\"video_rec_title\":\"\",\"video_rec_color\":\"\",\"video_rec_disable\":\"\",\"autoplay_vid\":\"yes\",\"show_vid_t\":\"block\",\"vid_t_margin\":\"\",\"vid_t_padding\":\"\",\"video_title_color\":\"\",\"video_title_color_h\":\"\",\"video_bg\":\"\",\"video_overlay\":\"\",\"vid_t_color\":\"\",\"vid_t_bg_color\":\"\",\"f_vid_title_font_header\":\"\",\"f_vid_title_font_title\":\"Video pop-up article title\",\"f_vid_title_font_settings\":\"\",\"f_vid_title_font_family\":\"\",\"f_vid_title_font_size\":\"\",\"f_vid_title_font_line_height\":\"\",\"f_vid_title_font_style\":\"\",\"f_vid_title_font_weight\":\"\",\"f_vid_title_font_transform\":\"\",\"f_vid_title_font_spacing\":\"\",\"f_vid_title_\":\"\",\"f_vid_time_font_title\":\"Video duration text\",\"f_vid_time_font_settings\":\"\",\"f_vid_time_font_family\":\"\",\"f_vid_time_font_size\":\"\",\"f_vid_time_font_line_height\":\"\",\"f_vid_time_font_style\":\"\",\"f_vid_time_font_weight\":\"\",\"f_vid_time_font_transform\":\"\",\"f_vid_time_font_spacing\":\"\",\"f_vid_time_\":\"\",\"meta_info_align\":\"\",\"meta_info_horiz\":\"layout-default\",\"meta_width\":\"\",\"meta_space\":\"\",\"art_btn\":\"\",\"meta_info_border_size\":\"\",\"meta_info_border_style\":\"\",\"meta_info_border_color\":\"#eaeaea\",\"meta_info_border_radius\":\"\",\"modules_category_padding\":\"\",\"modules_cat_border\":\"\",\"modules_category_radius\":\"0\",\"show_cat\":\"inline-block\",\"author_photo\":\"\",\"author_photo_size\":\"\",\"author_photo_space\":\"\",\"author_photo_radius\":\"\",\"show_date\":\"inline-block\",\"review_space\":\"\",\"review_size\":\"2.5\",\"review_distance\":\"\",\"excerpt_col\":\"1\",\"excerpt_gap\":\"\",\"excerpt_middle\":\"\",\"excerpt_inline\":\"\",\"show_audio\":\"block\",\"art_audio\":\"\",\"art_audio_size\":\"1.5\",\"btn_title\":\"\",\"btn_margin\":\"\",\"btn_padding\":\"\",\"btn_border_width\":\"\",\"btn_radius\":\"\",\"pag_space\":\"\",\"pag_padding\":\"\",\"pag_border_width\":\"\",\"pag_border_radius\":\"\",\"prev_tdicon\":\"\",\"next_tdicon\":\"\",\"pag_icons_size\":\"\",\"f_header_font_header\":\"\",\"f_header_font_title\":\"Block header\",\"f_header_font_settings\":\"\",\"f_header_font_family\":\"\",\"f_header_font_size\":\"\",\"f_header_font_line_height\":\"\",\"f_header_font_style\":\"\",\"f_header_font_weight\":\"\",\"f_header_font_transform\":\"\",\"f_header_font_spacing\":\"\",\"f_header_\":\"\",\"f_ajax_font_title\":\"Ajax categories\",\"f_ajax_font_settings\":\"\",\"f_ajax_font_family\":\"\",\"f_ajax_font_size\":\"\",\"f_ajax_font_line_height\":\"\",\"f_ajax_font_style\":\"\",\"f_ajax_font_weight\":\"\",\"f_ajax_font_transform\":\"\",\"f_ajax_font_spacing\":\"\",\"f_ajax_\":\"\",\"f_more_font_title\":\"Load more button\",\"f_more_font_settings\":\"\",\"f_more_font_family\":\"\",\"f_more_font_size\":\"\",\"f_more_font_line_height\":\"\",\"f_more_font_style\":\"\",\"f_more_font_weight\":\"\",\"f_more_font_transform\":\"\",\"f_more_font_spacing\":\"\",\"f_more_\":\"\",\"f_title_font_header\":\"\",\"f_title_font_title\":\"Article title\",\"f_title_font_settings\":\"\",\"f_title_font_line_height\":\"\",\"f_title_font_style\":\"\",\"f_title_font_spacing\":\"\",\"f_title_\":\"\",\"f_cat_font_title\":\"Article category tag\",\"f_cat_font_settings\":\"\",\"f_cat_font_family\":\"\",\"f_cat_font_size\":\"\",\"f_cat_font_line_height\":\"\",\"f_cat_font_style\":\"\",\"f_cat_font_weight\":\"\",\"f_cat_font_transform\":\"\",\"f_cat_font_spacing\":\"\",\"f_cat_\":\"\",\"f_meta_font_title\":\"Article meta info\",\"f_meta_font_settings\":\"\",\"f_meta_font_line_height\":\"\",\"f_meta_font_style\":\"\",\"f_meta_font_transform\":\"\",\"f_meta_font_spacing\":\"\",\"f_meta_\":\"\",\"f_ex_font_title\":\"Article excerpt\",\"f_ex_font_settings\":\"\",\"f_ex_font_line_height\":\"\",\"f_ex_font_style\":\"\",\"f_ex_font_weight\":\"\",\"f_ex_font_transform\":\"\",\"f_ex_font_spacing\":\"\",\"f_ex_\":\"\",\"f_btn_font_title\":\"Article read more button\",\"f_btn_font_settings\":\"\",\"f_btn_font_family\":\"\",\"f_btn_font_size\":\"\",\"f_btn_font_line_height\":\"\",\"f_btn_font_style\":\"\",\"f_btn_font_weight\":\"\",\"f_btn_font_transform\":\"\",\"f_btn_font_spacing\":\"\",\"f_btn_\":\"\",\"mix_color\":\"\",\"mix_type\":\"\",\"fe_brightness\":\"1\",\"fe_contrast\":\"1\",\"fe_saturate\":\"1\",\"mix_color_h\":\"\",\"mix_type_h\":\"\",\"fe_brightness_h\":\"1\",\"fe_contrast_h\":\"1\",\"fe_saturate_h\":\"1\",\"m_bg\":\"\",\"color_overlay\":\"\",\"shadow_shadow_header\":\"\",\"shadow_shadow_title\":\"Module Shadow\",\"shadow_shadow_size\":\"\",\"shadow_shadow_offset_horizontal\":\"\",\"shadow_shadow_offset_vertical\":\"\",\"shadow_shadow_spread\":\"\",\"shadow_shadow_color\":\"\",\"title_txt\":\"\",\"title_txt_hover\":\"\",\"all_underline_height\":\"\",\"all_underline_color\":\"\",\"cat_style\":\"\",\"cat_bg\":\"\",\"cat_bg_hover\":\"\",\"cat_txt\":\"\",\"cat_txt_hover\":\"\",\"cat_border\":\"\",\"cat_border_hover\":\"\",\"meta_bg\":\"\",\"author_txt\":\"\",\"author_txt_hover\":\"\",\"date_txt\":\"\",\"ex_txt\":\"\",\"com_bg\":\"\",\"com_txt\":\"\",\"rev_txt\":\"\",\"audio_btn_color\":\"\",\"audio_time_color\":\"\",\"audio_bar_color\":\"\",\"audio_bar_curr_color\":\"\",\"shadow_m_shadow_header\":\"\",\"shadow_m_shadow_title\":\"Meta info shadow\",\"shadow_m_shadow_size\":\"\",\"shadow_m_shadow_offset_horizontal\":\"\",\"shadow_m_shadow_offset_vertical\":\"\",\"shadow_m_shadow_spread\":\"\",\"shadow_m_shadow_color\":\"\",\"btn_bg\":\"\",\"btn_bg_hover\":\"\",\"btn_txt\":\"\",\"btn_txt_hover\":\"\",\"btn_border\":\"\",\"btn_border_hover\":\"\",\"pag_text\":\"\",\"pag_h_text\":\"\",\"pag_bg\":\"\",\"pag_h_bg\":\"\",\"pag_border\":\"\",\"pag_h_border\":\"\",\"ajax_pagination_next_prev_swipe\":\"\",\"ajax_pagination_infinite_stop\":\"\",\"css\":\"\",\"tdc_css\":\"\",\"td_column_number\":2,\"header_color\":\"\",\"color_preset\":\"\",\"border_top\":\"\",\"class\":\"tdi_77\",\"tdc_css_class\":\"tdi_77\",\"tdc_css_class_style\":\"tdi_77_rand_style\",\"search_query\":\"{current_keyword}\"}}",
            "td_block_id": "tdi_77",
            "td_column_number": "2",
            "td_current_page": f"{current_page}",
            "block_type": "td_flex_block_1",
            "td_filter_value": "",
            "td_user_action": "",
            "td_magic_token": "4a5ab8f3f5"
        }
        yield feapder.Request(url, params, data=data, callback=self.parse_url, page=current_page,
                              keyword=current_keyword,method="POST",
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
