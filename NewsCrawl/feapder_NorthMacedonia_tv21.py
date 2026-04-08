import json

import feapder
from feapder import Item
from lxml import etree
import re


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

    country = 'North Macedonia'
    table = 'North_Macedonia'
    #马其顿语
    keywords = ["Siklon tropikal", "Depresioni tropikal", "Stuhia tropikale", "Tajfun", "Uragan", "Ciklon", "Stuhia", "Shiua e rende", "Pllaja", "Përmbytje", "Dëmtimi bregdetar", "Perseri", "Katastrofa gjeologjike", "Katastrofa detare", "Era e forte", "Katastrofa e tajfunit", "Rebreshi i balteve", "Rebreshi i tokës"]

    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://mk.tv21.tv/wp-admin/admin-ajax.php"
            params = {
                "td_theme_name": "Newspaper",
                "v": "12.6.5"
            }
            data = {
                "action": "td_ajax_block",
                "td_atts": f"{{\"modules_on_row\":\"eyJhbGwiOiIzMy4zMzMzMzMzMyUiLCJwaG9uZSI6IjEwMCUifQ==\",\"modules_gap\":\"eyJsYW5kc2NhcGUiOiIxMCIsInBvcnRyYWl0IjoiMTAiLCJhbGwiOiIyMCJ9\",\"modules_category\":\"above\",\"show_excerpt\":\"none\",\"show_btn\":\"none\",\"ajax_pagination\":\"next_prev\",\"hide_audio\":\"yes\",\"limit\":\"9\",\"all_modules_space\":\"eyJhbGwiOiIzMCIsImxhbmRzY2FwZSI6IjI1IiwicG9ydHJhaXQiOiIyMCIsInBob25lIjoiMjAifQ==\",\"art_title\":\"eyJhbGwiOiIxMnB4IDAgMCAwIiwibGFuZHNjYXBlIjoiMTJweCAwIDAgMCIsInBvcnRyYWl0IjoiMTBweCAwIDAgMCJ9\",\"f_title_font_family\":\"672\",\"f_title_font_size\":\"eyJhbGwiOiIxOCIsInBob25lIjoiMjAiLCJsYW5kc2NhcGUiOiIxNyIsInBvcnRyYWl0IjoiMTQifQ==\",\"f_title_font_line_height\":\"1.2\",\"f_title_font_weight\":\"900\",\"title_txt\":\"#000000\",\"title_txt_hover\":\"#000000\",\"all_underline_color\":\"#19e1ff\",\"all_underline_height\":\"eyJhbGwiOiIyIiwicG9ydHJhaXQiOiIyIiwicGhvbmUiOiIzIn0=\",\"show_com\":\"none\",\"show_date\":\"none\",\"show_author\":\"none\",\"art_excerpt\":\"0\",\"image_height\":\"eyJhbGwiOiI2MCIsInBvcnRyYWl0IjoiNzAiLCJwaG9uZSI6IjUwIn0=\",\"image_radius\":\"\",\"modules_category_margin\":\"0\",\"modules_category_padding\":\"eyJhbGwiOiI2cHggMTBweCIsInBvcnRyYWl0IjoiNHB4IDZweCIsImxhbmRzY2FwZSI6IjVweCA4cHgifQ==\",\"f_cat_font_family\":\"672\",\"f_cat_font_size\":\"eyJhbGwiOiIxMiIsInBvcnRyYWl0IjoiMTAifQ==\",\"f_cat_font_line_height\":\"1\",\"f_cat_font_weight\":\"400\",\"f_cat_font_transform\":\"uppercase\",\"cat_bg\":\"#19e1ff\",\"cat_bg_hover\":\"#19e1ff\",\"cat_txt\":\"#ffffff\",\"cat_txt_hover\":\"#ffffff\",\"f_meta_font_family\":\"\",\"f_meta_font_size\":\"eyJwaG9uZSI6IjEyIn0=\",\"f_meta_font_weight\":\"\",\"f_meta_font_transform\":\"\",\"date_txt\":\"#555555\",\"f_ex_font_family\":\"\",\"f_ex_font_size\":\"eyJwaG9uZSI6IjE0In0=\",\"f_ex_font_weight\":\"\",\"ex_txt\":\"#555555\",\"meta_padding\":\"eyJhbGwiOiIyMHB4IDAgMCAxMHB4IiwicG9ydHJhaXQiOiIxNXB4IDAgMCAxMHB4In0=\",\"tdc_css\":\"eyJhbGwiOnsibWFyZ2luLWJvdHRvbSI6IjAiLCJkaXNwbGF5IjoiIn19\",\"pag_border_width\":\"0\",\"pag_text\":\"#000000\",\"pag_h_text\":\"#19e1ff\",\"pag_a_text\":\"#19e1ff\",\"pag_bg\":\"rgba(255,255,255,0)\",\"pag_h_bg\":\"rgba(255,255,255,0)\",\"pag_a_bg\":\"rgba(255,255,255,0)\",\"f_pag_font_family\":\"672\",\"f_pag_font_size\":\"12\",\"f_pag_font_transform\":\"uppercase\",\"f_pag_font_weight\":\"700\",\"search_query\":\"{keyword}\",\"block_type\":\"tdb_loop\",\"separator\":\"\",\"custom_title\":\"\",\"custom_url\":\"\",\"block_template_id\":\"\",\"title_tag\":\"\",\"mc1_tl\":\"\",\"mc1_title_tag\":\"\",\"mc1_el\":\"\",\"offset\":\"\",\"open_in_new_window\":\"\",\"post_ids\":\"-284724\",\"include_cf_posts\":\"\",\"exclude_cf_posts\":\"\",\"sort\":\"\",\"installed_post_types\":\"\",\"ajax_pagination_next_prev_swipe\":\"\",\"ajax_pagination_infinite_stop\":\"\",\"review_source\":\"\",\"container_width\":\"\",\"m_padding\":\"\",\"m_radius\":\"\",\"modules_border_size\":\"\",\"modules_border_style\":\"\",\"modules_border_color\":\"#eaeaea\",\"modules_divider\":\"\",\"modules_divider_color\":\"#eaeaea\",\"h_effect\":\"\",\"image_size\":\"\",\"image_alignment\":\"50\",\"image_width\":\"\",\"image_floated\":\"no_float\",\"hide_image\":\"\",\"show_favourites\":\"\",\"fav_size\":\"2\",\"fav_space\":\"\",\"fav_ico_color\":\"\",\"fav_ico_color_h\":\"\",\"fav_bg\":\"\",\"fav_bg_h\":\"\",\"fav_shadow_shadow_header\":\"\",\"fav_shadow_shadow_title\":\"Shadow\",\"fav_shadow_shadow_size\":\"\",\"fav_shadow_shadow_offset_horizontal\":\"\",\"fav_shadow_shadow_offset_vertical\":\"\",\"fav_shadow_shadow_spread\":\"\",\"fav_shadow_shadow_color\":\"\",\"video_icon\":\"\",\"video_popup\":\"yes\",\"video_rec\":\"\",\"spot_header\":\"\",\"video_rec_title\":\"- Advertisement -\",\"video_rec_color\":\"\",\"video_rec_disable\":\"\",\"autoplay_vid\":\"yes\",\"show_vid_t\":\"block\",\"vid_t_margin\":\"\",\"vid_t_padding\":\"\",\"video_title_color\":\"\",\"video_title_color_h\":\"\",\"video_bg\":\"\",\"video_overlay\":\"\",\"vid_t_color\":\"\",\"vid_t_bg_color\":\"\",\"f_vid_title_font_header\":\"\",\"f_vid_title_font_title\":\"Video pop-up article title\",\"f_vid_title_font_settings\":\"\",\"f_vid_title_font_family\":\"\",\"f_vid_title_font_size\":\"\",\"f_vid_title_font_line_height\":\"\",\"f_vid_title_font_style\":\"\",\"f_vid_title_font_weight\":\"\",\"f_vid_title_font_transform\":\"\",\"f_vid_title_font_spacing\":\"\",\"f_vid_title_\":\"\",\"f_vid_time_font_title\":\"Video duration text\",\"f_vid_time_font_settings\":\"\",\"f_vid_time_font_family\":\"\",\"f_vid_time_font_size\":\"\",\"f_vid_time_font_line_height\":\"\",\"f_vid_time_font_style\":\"\",\"f_vid_time_font_weight\":\"\",\"f_vid_time_font_transform\":\"\",\"f_vid_time_font_spacing\":\"\",\"f_vid_time_\":\"\",\"meta_info_align\":\"\",\"meta_info_horiz\":\"content-horiz-left\",\"meta_width\":\"\",\"meta_margin\":\"\",\"meta_space\":\"\",\"meta_info_border_size\":\"\",\"meta_info_border_style\":\"\",\"meta_info_border_color\":\"#eaeaea\",\"meta_info_border_radius\":\"\",\"art_btn\":\"\",\"modules_cat_border\":\"\",\"modules_category_radius\":\"0\",\"show_cat\":\"inline-block\",\"modules_extra_cat\":\"\",\"author_photo\":\"\",\"author_photo_size\":\"\",\"author_photo_space\":\"\",\"author_photo_radius\":\"\",\"show_modified_date\":\"\",\"time_ago\":\"\",\"time_ago_add_txt\":\"ago\",\"time_ago_txt_pos\":\"\",\"show_review\":\"inline-block\",\"review_space\":\"\",\"review_size\":\"2.5\",\"review_distance\":\"\",\"excerpt_col\":\"1\",\"excerpt_gap\":\"\",\"excerpt_middle\":\"\",\"excerpt_inline\":\"\",\"show_audio\":\"block\",\"art_audio\":\"\",\"art_audio_size\":\"1.5\",\"btn_title\":\"\",\"btn_margin\":\"\",\"btn_padding\":\"\",\"btn_border_width\":\"\",\"btn_radius\":\"\",\"pag_space\":\"\",\"pag_padding\":\"\",\"pag_border_radius\":\"\",\"prev_tdicon\":\"\",\"next_tdicon\":\"\",\"pag_icons_size\":\"\",\"f_header_font_header\":\"\",\"f_header_font_title\":\"Block header\",\"f_header_font_settings\":\"\",\"f_header_font_family\":\"\",\"f_header_font_size\":\"\",\"f_header_font_line_height\":\"\",\"f_header_font_style\":\"\",\"f_header_font_weight\":\"\",\"f_header_font_transform\":\"\",\"f_header_font_spacing\":\"\",\"f_header_\":\"\",\"f_pag_font_title\":\"Pagination text\",\"f_pag_font_settings\":\"\",\"f_pag_font_line_height\":\"\",\"f_pag_font_style\":\"\",\"f_pag_font_spacing\":\"\",\"f_pag_\":\"\",\"f_title_font_header\":\"\",\"f_title_font_title\":\"Article title\",\"f_title_font_settings\":\"\",\"f_title_font_style\":\"\",\"f_title_font_transform\":\"\",\"f_title_font_spacing\":\"\",\"f_title_\":\"\",\"f_cat_font_title\":\"Article category tag\",\"f_cat_font_settings\":\"\",\"f_cat_font_style\":\"\",\"f_cat_font_spacing\":\"\",\"f_cat_\":\"\",\"f_meta_font_title\":\"Article meta info\",\"f_meta_font_settings\":\"\",\"f_meta_font_line_height\":\"\",\"f_meta_font_style\":\"\",\"f_meta_font_spacing\":\"\",\"f_meta_\":\"\",\"f_ex_font_title\":\"Article excerpt\",\"f_ex_font_settings\":\"\",\"f_ex_font_line_height\":\"\",\"f_ex_font_style\":\"\",\"f_ex_font_transform\":\"\",\"f_ex_font_spacing\":\"\",\"f_ex_\":\"\",\"f_btn_font_title\":\"Article read more button\",\"f_btn_font_settings\":\"\",\"f_btn_font_family\":\"\",\"f_btn_font_size\":\"\",\"f_btn_font_line_height\":\"\",\"f_btn_font_style\":\"\",\"f_btn_font_weight\":\"\",\"f_btn_font_transform\":\"\",\"f_btn_font_spacing\":\"\",\"f_btn_\":\"\",\"mix_color\":\"\",\"mix_type\":\"\",\"fe_brightness\":\"1\",\"fe_contrast\":\"1\",\"fe_saturate\":\"1\",\"mix_color_h\":\"\",\"mix_type_h\":\"\",\"fe_brightness_h\":\"1\",\"fe_contrast_h\":\"1\",\"fe_saturate_h\":\"1\",\"m_bg\":\"\",\"shadow_shadow_header\":\"\",\"shadow_shadow_title\":\"Module Shadow\",\"shadow_shadow_size\":\"\",\"shadow_shadow_offset_horizontal\":\"\",\"shadow_shadow_offset_vertical\":\"\",\"shadow_shadow_spread\":\"\",\"shadow_shadow_color\":\"\",\"cat_border\":\"\",\"cat_border_hover\":\"\",\"meta_bg\":\"\",\"author_txt\":\"\",\"author_txt_hover\":\"\",\"com_bg\":\"\",\"com_txt\":\"\",\"rev_txt\":\"\",\"shadow_m_shadow_header\":\"\",\"shadow_m_shadow_title\":\"Meta info shadow\",\"shadow_m_shadow_size\":\"\",\"shadow_m_shadow_offset_horizontal\":\"\",\"shadow_m_shadow_offset_vertical\":\"\",\"shadow_m_shadow_spread\":\"\",\"shadow_m_shadow_color\":\"\",\"audio_btn_color\":\"\",\"audio_time_color\":\"\",\"audio_bar_color\":\"\",\"audio_bar_curr_color\":\"\",\"btn_bg\":\"\",\"btn_bg_hover\":\"\",\"btn_txt\":\"\",\"btn_txt_hover\":\"\",\"btn_border\":\"\",\"btn_border_hover\":\"\",\"nextprev_border_h\":\"\",\"pag_border\":\"\",\"pag_h_border\":\"\",\"pag_a_border\":\"\",\"ad_loop\":\"\",\"ad_loop_title\":\"- Advertisement -\",\"ad_loop_repeat\":\"\",\"ad_loop_color\":\"\",\"ad_loop_full\":\"yes\",\"f_ad_font_header\":\"\",\"f_ad_font_title\":\"Ad title text\",\"f_ad_font_settings\":\"\",\"f_ad_font_family\":\"\",\"f_ad_font_size\":\"\",\"f_ad_font_line_height\":\"\",\"f_ad_font_style\":\"\",\"f_ad_font_weight\":\"\",\"f_ad_font_transform\":\"\",\"f_ad_font_spacing\":\"\",\"f_ad_\":\"\",\"ad_loop_disable\":\"\",\"el_class\":\"\",\"td_column_number\":2,\"header_color\":\"\",\"td_ajax_preloading\":\"\",\"td_ajax_filter_type\":\"\",\"td_filter_default_txt\":\"\",\"td_ajax_filter_ids\":\"\",\"color_preset\":\"\",\"border_top\":\"\",\"css\":\"\",\"class\":\"tdi_69\",\"tdc_css_class\":\"tdi_69\",\"tdc_css_class_style\":\"tdi_69_rand_style\"}}",
                "td_block_id": "tdi_69",
                "td_column_number": "2",
                "td_current_page": "1",
                "block_type": "tdb_loop",
                "td_filter_value": "",
                "td_user_action": "",
                "td_magic_token": "f1abc413b7"
            }
            yield feapder.Request(url, data=data, params=params, callback=self.parse_url, page=page, keyword=keyword)

    def download_midware(self, request):
        request.headers = {
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://mk.tv21.tv",
            "priority": "u=1, i",
            "referer": "https://mk.tv21.tv/?s=%D0%B2%D0%B8%D1%81%D0%BE%D0%BA%D0%B0+%D1%82%D0%BE%D0%BF%D0%BB%D0%B8%D0%BD%D0%B0",
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
            "_ga": "GA1.1.262637506.1736322998",
            "_fbp": "fb.1.1736322998651.54159920082022061",
            "FCNEC": "%5B%5B%22AKsRol8GrA1kNE_Ce3q77D96Z1AcJ_L-AM40zcA6r3hElg2EBebHNlcH-mXP69dS_7Scu8hHaLIM_gPUr7H4HqEceHJWx6UJaDcbveDfItkgcls6Nd_g3cezi6EEedZE3tFekJeElO08CdFQt13tlSeACbLBBtaV4A%3D%3D%22%5D%5D",
            "_ga_2CY7D6GRHQ": "GS1.1.1736322998.1.1.1736323312.50.0.505858176"
        }
        return request

    def parse_url(self, request, response):
        # print(response.text)
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        data = response.json.get("td_data")
        from lxml import etree
        html = etree.HTML(data)
        links = html.xpath("//h3[@class='entry-title td-module-title']/a/@href")
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
            print(item)
            items = Item()
            items.table_name = self.table
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://mk.tv21.tv/wp-admin/admin-ajax.php"
        params = {
            "td_theme_name": "Newspaper",
            "v": "12.6.5"
        }
        data = {
            "action": "td_ajax_block",
            "td_atts": f"{{\"modules_on_row\":\"eyJhbGwiOiIzMy4zMzMzMzMzMyUiLCJwaG9uZSI6IjEwMCUifQ==\",\"modules_gap\":\"eyJsYW5kc2NhcGUiOiIxMCIsInBvcnRyYWl0IjoiMTAiLCJhbGwiOiIyMCJ9\",\"modules_category\":\"above\",\"show_excerpt\":\"none\",\"show_btn\":\"none\",\"ajax_pagination\":\"next_prev\",\"hide_audio\":\"yes\",\"limit\":\"9\",\"all_modules_space\":\"eyJhbGwiOiIzMCIsImxhbmRzY2FwZSI6IjI1IiwicG9ydHJhaXQiOiIyMCIsInBob25lIjoiMjAifQ==\",\"art_title\":\"eyJhbGwiOiIxMnB4IDAgMCAwIiwibGFuZHNjYXBlIjoiMTJweCAwIDAgMCIsInBvcnRyYWl0IjoiMTBweCAwIDAgMCJ9\",\"f_title_font_family\":\"672\",\"f_title_font_size\":\"eyJhbGwiOiIxOCIsInBob25lIjoiMjAiLCJsYW5kc2NhcGUiOiIxNyIsInBvcnRyYWl0IjoiMTQifQ==\",\"f_title_font_line_height\":\"1.2\",\"f_title_font_weight\":\"900\",\"title_txt\":\"#000000\",\"title_txt_hover\":\"#000000\",\"all_underline_color\":\"#19e1ff\",\"all_underline_height\":\"eyJhbGwiOiIyIiwicG9ydHJhaXQiOiIyIiwicGhvbmUiOiIzIn0=\",\"show_com\":\"none\",\"show_date\":\"none\",\"show_author\":\"none\",\"art_excerpt\":\"0\",\"image_height\":\"eyJhbGwiOiI2MCIsInBvcnRyYWl0IjoiNzAiLCJwaG9uZSI6IjUwIn0=\",\"image_radius\":\"\",\"modules_category_margin\":\"0\",\"modules_category_padding\":\"eyJhbGwiOiI2cHggMTBweCIsInBvcnRyYWl0IjoiNHB4IDZweCIsImxhbmRzY2FwZSI6IjVweCA4cHgifQ==\",\"f_cat_font_family\":\"672\",\"f_cat_font_size\":\"eyJhbGwiOiIxMiIsInBvcnRyYWl0IjoiMTAifQ==\",\"f_cat_font_line_height\":\"1\",\"f_cat_font_weight\":\"400\",\"f_cat_font_transform\":\"uppercase\",\"cat_bg\":\"#19e1ff\",\"cat_bg_hover\":\"#19e1ff\",\"cat_txt\":\"#ffffff\",\"cat_txt_hover\":\"#ffffff\",\"f_meta_font_family\":\"\",\"f_meta_font_size\":\"eyJwaG9uZSI6IjEyIn0=\",\"f_meta_font_weight\":\"\",\"f_meta_font_transform\":\"\",\"date_txt\":\"#555555\",\"f_ex_font_family\":\"\",\"f_ex_font_size\":\"eyJwaG9uZSI6IjE0In0=\",\"f_ex_font_weight\":\"\",\"ex_txt\":\"#555555\",\"meta_padding\":\"eyJhbGwiOiIyMHB4IDAgMCAxMHB4IiwicG9ydHJhaXQiOiIxNXB4IDAgMCAxMHB4In0=\",\"tdc_css\":\"eyJhbGwiOnsibWFyZ2luLWJvdHRvbSI6IjAiLCJkaXNwbGF5IjoiIn19\",\"pag_border_width\":\"0\",\"pag_text\":\"#000000\",\"pag_h_text\":\"#19e1ff\",\"pag_a_text\":\"#19e1ff\",\"pag_bg\":\"rgba(255,255,255,0)\",\"pag_h_bg\":\"rgba(255,255,255,0)\",\"pag_a_bg\":\"rgba(255,255,255,0)\",\"f_pag_font_family\":\"672\",\"f_pag_font_size\":\"12\",\"f_pag_font_transform\":\"uppercase\",\"f_pag_font_weight\":\"700\",\"search_query\":\"{current_keyword}\",\"block_type\":\"tdb_loop\",\"separator\":\"\",\"custom_title\":\"\",\"custom_url\":\"\",\"block_template_id\":\"\",\"title_tag\":\"\",\"mc1_tl\":\"\",\"mc1_title_tag\":\"\",\"mc1_el\":\"\",\"offset\":\"\",\"open_in_new_window\":\"\",\"post_ids\":\"-284724\",\"include_cf_posts\":\"\",\"exclude_cf_posts\":\"\",\"sort\":\"\",\"installed_post_types\":\"\",\"ajax_pagination_next_prev_swipe\":\"\",\"ajax_pagination_infinite_stop\":\"\",\"review_source\":\"\",\"container_width\":\"\",\"m_padding\":\"\",\"m_radius\":\"\",\"modules_border_size\":\"\",\"modules_border_style\":\"\",\"modules_border_color\":\"#eaeaea\",\"modules_divider\":\"\",\"modules_divider_color\":\"#eaeaea\",\"h_effect\":\"\",\"image_size\":\"\",\"image_alignment\":\"50\",\"image_width\":\"\",\"image_floated\":\"no_float\",\"hide_image\":\"\",\"show_favourites\":\"\",\"fav_size\":\"2\",\"fav_space\":\"\",\"fav_ico_color\":\"\",\"fav_ico_color_h\":\"\",\"fav_bg\":\"\",\"fav_bg_h\":\"\",\"fav_shadow_shadow_header\":\"\",\"fav_shadow_shadow_title\":\"Shadow\",\"fav_shadow_shadow_size\":\"\",\"fav_shadow_shadow_offset_horizontal\":\"\",\"fav_shadow_shadow_offset_vertical\":\"\",\"fav_shadow_shadow_spread\":\"\",\"fav_shadow_shadow_color\":\"\",\"video_icon\":\"\",\"video_popup\":\"yes\",\"video_rec\":\"\",\"spot_header\":\"\",\"video_rec_title\":\"- Advertisement -\",\"video_rec_color\":\"\",\"video_rec_disable\":\"\",\"autoplay_vid\":\"yes\",\"show_vid_t\":\"block\",\"vid_t_margin\":\"\",\"vid_t_padding\":\"\",\"video_title_color\":\"\",\"video_title_color_h\":\"\",\"video_bg\":\"\",\"video_overlay\":\"\",\"vid_t_color\":\"\",\"vid_t_bg_color\":\"\",\"f_vid_title_font_header\":\"\",\"f_vid_title_font_title\":\"Video pop-up article title\",\"f_vid_title_font_settings\":\"\",\"f_vid_title_font_family\":\"\",\"f_vid_title_font_size\":\"\",\"f_vid_title_font_line_height\":\"\",\"f_vid_title_font_style\":\"\",\"f_vid_title_font_weight\":\"\",\"f_vid_title_font_transform\":\"\",\"f_vid_title_font_spacing\":\"\",\"f_vid_title_\":\"\",\"f_vid_time_font_title\":\"Video duration text\",\"f_vid_time_font_settings\":\"\",\"f_vid_time_font_family\":\"\",\"f_vid_time_font_size\":\"\",\"f_vid_time_font_line_height\":\"\",\"f_vid_time_font_style\":\"\",\"f_vid_time_font_weight\":\"\",\"f_vid_time_font_transform\":\"\",\"f_vid_time_font_spacing\":\"\",\"f_vid_time_\":\"\",\"meta_info_align\":\"\",\"meta_info_horiz\":\"content-horiz-left\",\"meta_width\":\"\",\"meta_margin\":\"\",\"meta_space\":\"\",\"meta_info_border_size\":\"\",\"meta_info_border_style\":\"\",\"meta_info_border_color\":\"#eaeaea\",\"meta_info_border_radius\":\"\",\"art_btn\":\"\",\"modules_cat_border\":\"\",\"modules_category_radius\":\"0\",\"show_cat\":\"inline-block\",\"modules_extra_cat\":\"\",\"author_photo\":\"\",\"author_photo_size\":\"\",\"author_photo_space\":\"\",\"author_photo_radius\":\"\",\"show_modified_date\":\"\",\"time_ago\":\"\",\"time_ago_add_txt\":\"ago\",\"time_ago_txt_pos\":\"\",\"show_review\":\"inline-block\",\"review_space\":\"\",\"review_size\":\"2.5\",\"review_distance\":\"\",\"excerpt_col\":\"1\",\"excerpt_gap\":\"\",\"excerpt_middle\":\"\",\"excerpt_inline\":\"\",\"show_audio\":\"block\",\"art_audio\":\"\",\"art_audio_size\":\"1.5\",\"btn_title\":\"\",\"btn_margin\":\"\",\"btn_padding\":\"\",\"btn_border_width\":\"\",\"btn_radius\":\"\",\"pag_space\":\"\",\"pag_padding\":\"\",\"pag_border_radius\":\"\",\"prev_tdicon\":\"\",\"next_tdicon\":\"\",\"pag_icons_size\":\"\",\"f_header_font_header\":\"\",\"f_header_font_title\":\"Block header\",\"f_header_font_settings\":\"\",\"f_header_font_family\":\"\",\"f_header_font_size\":\"\",\"f_header_font_line_height\":\"\",\"f_header_font_style\":\"\",\"f_header_font_weight\":\"\",\"f_header_font_transform\":\"\",\"f_header_font_spacing\":\"\",\"f_header_\":\"\",\"f_pag_font_title\":\"Pagination text\",\"f_pag_font_settings\":\"\",\"f_pag_font_line_height\":\"\",\"f_pag_font_style\":\"\",\"f_pag_font_spacing\":\"\",\"f_pag_\":\"\",\"f_title_font_header\":\"\",\"f_title_font_title\":\"Article title\",\"f_title_font_settings\":\"\",\"f_title_font_style\":\"\",\"f_title_font_transform\":\"\",\"f_title_font_spacing\":\"\",\"f_title_\":\"\",\"f_cat_font_title\":\"Article category tag\",\"f_cat_font_settings\":\"\",\"f_cat_font_style\":\"\",\"f_cat_font_spacing\":\"\",\"f_cat_\":\"\",\"f_meta_font_title\":\"Article meta info\",\"f_meta_font_settings\":\"\",\"f_meta_font_line_height\":\"\",\"f_meta_font_style\":\"\",\"f_meta_font_spacing\":\"\",\"f_meta_\":\"\",\"f_ex_font_title\":\"Article excerpt\",\"f_ex_font_settings\":\"\",\"f_ex_font_line_height\":\"\",\"f_ex_font_style\":\"\",\"f_ex_font_transform\":\"\",\"f_ex_font_spacing\":\"\",\"f_ex_\":\"\",\"f_btn_font_title\":\"Article read more button\",\"f_btn_font_settings\":\"\",\"f_btn_font_family\":\"\",\"f_btn_font_size\":\"\",\"f_btn_font_line_height\":\"\",\"f_btn_font_style\":\"\",\"f_btn_font_weight\":\"\",\"f_btn_font_transform\":\"\",\"f_btn_font_spacing\":\"\",\"f_btn_\":\"\",\"mix_color\":\"\",\"mix_type\":\"\",\"fe_brightness\":\"1\",\"fe_contrast\":\"1\",\"fe_saturate\":\"1\",\"mix_color_h\":\"\",\"mix_type_h\":\"\",\"fe_brightness_h\":\"1\",\"fe_contrast_h\":\"1\",\"fe_saturate_h\":\"1\",\"m_bg\":\"\",\"shadow_shadow_header\":\"\",\"shadow_shadow_title\":\"Module Shadow\",\"shadow_shadow_size\":\"\",\"shadow_shadow_offset_horizontal\":\"\",\"shadow_shadow_offset_vertical\":\"\",\"shadow_shadow_spread\":\"\",\"shadow_shadow_color\":\"\",\"cat_border\":\"\",\"cat_border_hover\":\"\",\"meta_bg\":\"\",\"author_txt\":\"\",\"author_txt_hover\":\"\",\"com_bg\":\"\",\"com_txt\":\"\",\"rev_txt\":\"\",\"shadow_m_shadow_header\":\"\",\"shadow_m_shadow_title\":\"Meta info shadow\",\"shadow_m_shadow_size\":\"\",\"shadow_m_shadow_offset_horizontal\":\"\",\"shadow_m_shadow_offset_vertical\":\"\",\"shadow_m_shadow_spread\":\"\",\"shadow_m_shadow_color\":\"\",\"audio_btn_color\":\"\",\"audio_time_color\":\"\",\"audio_bar_color\":\"\",\"audio_bar_curr_color\":\"\",\"btn_bg\":\"\",\"btn_bg_hover\":\"\",\"btn_txt\":\"\",\"btn_txt_hover\":\"\",\"btn_border\":\"\",\"btn_border_hover\":\"\",\"nextprev_border_h\":\"\",\"pag_border\":\"\",\"pag_h_border\":\"\",\"pag_a_border\":\"\",\"ad_loop\":\"\",\"ad_loop_title\":\"- Advertisement -\",\"ad_loop_repeat\":\"\",\"ad_loop_color\":\"\",\"ad_loop_full\":\"yes\",\"f_ad_font_header\":\"\",\"f_ad_font_title\":\"Ad title text\",\"f_ad_font_settings\":\"\",\"f_ad_font_family\":\"\",\"f_ad_font_size\":\"\",\"f_ad_font_line_height\":\"\",\"f_ad_font_style\":\"\",\"f_ad_font_weight\":\"\",\"f_ad_font_transform\":\"\",\"f_ad_font_spacing\":\"\",\"f_ad_\":\"\",\"ad_loop_disable\":\"\",\"el_class\":\"\",\"td_column_number\":2,\"header_color\":\"\",\"td_ajax_preloading\":\"\",\"td_ajax_filter_type\":\"\",\"td_filter_default_txt\":\"\",\"td_ajax_filter_ids\":\"\",\"color_preset\":\"\",\"border_top\":\"\",\"css\":\"\",\"class\":\"tdi_69\",\"tdc_css_class\":\"tdi_69\",\"tdc_css_class_style\":\"tdi_69_rand_style\"}}",
            "td_block_id": "tdi_69",
            "td_column_number": "2",
            "td_current_page": f"{current_page}",
            "block_type": "tdb_loop",
            "td_filter_value": "",
            "td_user_action": "",
            "td_magic_token": "f1abc413b7"
        }
        yield feapder.Request(url, data=data,params=params, callback=self.parse_url, page=current_page, keyword=current_keyword)

    def parse_detail(self, request, response):
        items = request.items
        items.table_name = self.table
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='tdb-block-inner td-fix-index']//p/text()").extract())
        items.author = ''
        items.pubtime = ''
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
