# -*- coding: utf-8 -*-
"""

集群运行

"""
import re

import feapder
from feapder import Item
import json
from lxml import etree

from feapder.db.mysqldb import MysqlDB

class AirSpiderDemo(feapder.AirSpider):
    db = MysqlDB(
        ip="192.168.101.200", port=3307, db="spider_keywords", user_name="czm", user_pass="root"
    )
    sql = """ select db_name from keywords where country='Pakistan' and language = '英语'"""
    mysql_db = db.find(sql, to_json=True)[0].get("db_name")
    print("待写入的数据库是:", mysql_db)
    # 判断数据库是否存在
    db.execute(f"CREATE DATABASE IF NOT EXISTS `{mysql_db}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")
    __custom_setting__ = dict(

        SPIDER_THREAD_COUNT=5,  # 爬虫并发数，追求速度推荐32
        # # 下载时间间隔 单位秒。 支持随机 如 SPIDER_SLEEP_TIME = [2, 5] 则间隔为 2~5秒之间的随机数，包含2和5
        SPIDER_SLEEP_TIME=[8, 10],
        SPIDER_MAX_RETRY_TIMES=1,  # 每个请求最大重试次数
        MYSQL_IP="192.168.101.200",
        MYSQL_PORT=3307,
        MYSQL_DB=f"{mysql_db}",
        MYSQL_USER_NAME="czm",
        MYSQL_USER_PASS="root",
        ITEM_FILTER_ENABLE=True,  # item 去重
        ITEM_FILTER_SETTING=dict(
            filter_type=4  # 永久去重（BloomFilter） = 1 、内存去重（MemoryFilter） = 2、 临时去重（ExpireFilter）= 3、轻量去重（LiteFilter）= 4
        )
    )

    previous_links = None

    country = 'Pakistan'
    table = 'Pakistan'
    # 英文
    create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS `{mysql_db}`.`{table}`  (
              `id` int NOT NULL AUTO_INCREMENT,
              `title` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '标题',
              `author` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '作者',
              `keyword` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '关键词',
              `content` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '内容',
              `article_url` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '文章网址',
              `pubtime` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '发布时间',
              `country` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '国家',
              `news_source_country` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '新闻来源国家',
              `place` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '地名',
              `Longitude_latitude` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '经纬度',
              `english` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL COMMENT '英文',
              `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '写入时间',
              PRIMARY KEY (`id`) USING BTREE,
              UNIQUE INDEX `title_uni`(`keyword` ASC, `article_url` ASC) USING BTREE
            ) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

                """
    db.execute(create_table_sql)
    print(f"{table}创建成功<=================")
    keywords = db.find(f"select keywords_list from keywords where language = '英语' and country='{table}'", to_json=True)[0].get(
        "keywords_list")
    print("待抓取的关键词列===========>", keywords)
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://arynews.tv/wp-admin/admin-ajax.php"
            params = {
                "td_theme_name": "Newspaper",
                "v": "12.6.2"
            }

            headers_ = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                "cache-control": "max-age=0",
                "priority": "u=0, i",
                "referer": "https://arynews.tv/",
                "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Microsoft Edge\";v=\"133\", \"Chromium\";v=\"133\"",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": "\"Windows\"",
                "sec-fetch-dest": "document",
                "sec-fetch-mode": "navigate",
                "sec-fetch-site": "same-origin",
                "sec-fetch-user": "?1",
                "upgrade-insecure-requests": "1",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0"
            }
            url_ = "https://arynews.tv/"
            params_ = {
                "s": f"{keyword}"
            }
            import requests
            response = requests.get(url_, headers=headers_, params=params_).text
            match = re.search(r'tdBlockNonce\s*=\s*"([^"]+)"', response)
            td_magic_token = ''
            if match:
                value = match.group(1)
                td_magic_token = value
                print(value)  # 输出: b3803417b9

            data = {
                "action": "td_ajax_block",
                "td_atts": f"{{\"modules_on_row\":\"50%\",\"modules_gap\":\"10\",\"modules_category\":\"above\",\"show_excerpt\":\"eyJwb3J0cmFpdCI6Im5vbmUiLCJhbGwiOiJub25lIn0=\",\"show_btn\":\"none\",\"ajax_pagination\":\"load_more\",\"hide_audio\":\"yes\",\"limit\":\"20\",\"image_width\":\"eyJhbGwiOiIzMCIsInBob25lIjoiMTAwIn0=\",\"image_floated\":\"eyJhbGwiOiJmbG9hdF9sZWZ0IiwicGhvbmUiOiJub19mbG9hdCJ9\",\"meta_padding\":\"eyJhbGwiOiIwIDAgMCAzMHB4IiwibGFuZHNjYXBlIjoiMCAwIDAgMjVweCIsInBvcnRyYWl0IjoiMCAwIDAgMjBweCIsInBob25lIjoiMjVweCAwIDAgMCJ9\",\"image_radius\":\"10\",\"image_height\":\"eyJwaG9uZSI6IjExMCIsImFsbCI6Ijc1In0=\",\"meta_info_horiz\":\"\",\"modules_category_margin\":\"eyJhbGwiOiIwIiwicG9ydHJhaXQiOiIwIn0=\",\"show_cat\":\"none\",\"show_author\":\"none\",\"show_com\":\"none\",\"show_review\":\"none\",\"show_date\":\"none\",\"art_title\":\"eyJhbGwiOiIxMHB4IDAiLCJwb3J0cmFpdCI6IjZweCAwIiwibGFuZHNjYXBlIjoiOHB4IDAifQ==\",\"f_title_font_family\":\"downtown-sans-serif-font_global\",\"f_title_font_size\":\"eyJhbGwiOiIyMiIsImxhbmRzY2FwZSI6IjI4IiwicG9ydHJhaXQiOiIyNCIsInBob25lIjoiMzIifQ==\",\"f_title_font_line_height\":\"1\",\"f_title_font_weight\":\"900\",\"f_title_font_transform\":\"undefined\",\"title_txt\":\"#000000\",\"title_txt_hover\":\"#444444\",\"modules_category_padding\":\"eyJhbGwiOiIwIiwicG9ydHJhaXQiOiIwIn0=\",\"f_cat_font_family\":\"downtown-sans-serif-font_global\",\"f_cat_font_size\":\"eyJhbGwiOiIxNSIsImxhbmRzY2FwZSI6IjE0IiwicG9ydHJhaXQiOiIxMyJ9\",\"f_cat_font_line_height\":\"1\",\"f_cat_font_weight\":\"700\",\"f_cat_font_transform\":\"\",\"cat_bg\":\"rgba(255,255,255,0)\",\"cat_bg_hover\":\"rgba(255,255,255,0)\",\"cat_txt\":\"#edb500\",\"author_txt\":\"undefined\",\"author_txt_hover\":\"undefined\",\"art_excerpt\":\"eyJwb3J0cmFpdCI6IjZweCAwIDAgMCIsImFsbCI6IjAifQ==\",\"f_ex_font_family\":\"downtown-sans-serif-font_global\",\"f_ex_font_size\":\"eyJhbGwiOiIxNSIsImxhbmRzY2FwZSI6IjE0IiwicG9ydHJhaXQiOiIxMyJ9\",\"f_ex_font_line_height\":\"1.4\",\"f_ex_font_weight\":\"500\",\"ex_txt\":\"#666666\",\"meta_info_align\":\"center\",\"all_modules_space\":\"eyJhbGwiOiIyMCIsInBvcnRyYWl0IjoiMTAiLCJsYW5kc2NhcGUiOiIxNSIsInBob25lIjoiMzAifQ==\",\"tdc_css\":\"eyJhbGwiOnsibWFyZ2luLWJvdHRvbSI6IjAiLCJkaXNwbGF5IjoiIn0sImxhbmRzY2FwZSI6eyJkaXNwbGF5IjoiIn0sImxhbmRzY2FwZV9tYXhfd2lkdGgiOjExNDAsImxhbmRzY2FwZV9taW5fd2lkdGgiOjEwMTksInBvcnRyYWl0Ijp7ImRpc3BsYXkiOiIifSwicG9ydHJhaXRfbWF4X3dpZHRoIjoxMDE4LCJwb3J0cmFpdF9taW5fd2lkdGgiOjc2OCwicGhvbmUiOnsiZGlzcGxheSI6IiJ9LCJwaG9uZV9tYXhfd2lkdGgiOjc2N30=\",\"pag_h_bg\":\"var(--downtown-menu-bg)\",\"pag_a_bg\":\"var(--downtown-menu-bg)\",\"modules_cat_border\":\"0\",\"modules_category_radius\":\"50\",\"f_cat_font_style\":\"undefined\",\"f_cat_font_spacing\":\"undefined\",\"cat_txt_hover\":\"#000000\",\"f_title_font_style\":\"undefined\",\"f_title_font_spacing\":\"undefined\",\"f_meta_font_family\":\"downtown-sans-serif-font_global\",\"f_meta_font_size\":\"eyJhbGwiOiIxMyIsInBvcnRyYWl0IjoiMTIifQ==\",\"f_meta_font_line_height\":\"1\",\"f_meta_font_style\":\"undefined\",\"f_meta_font_weight\":\"500\",\"f_meta_font_transform\":\"capitalize\",\"f_meta_font_spacing\":\"undefined\",\"date_txt\":\"#666666\",\"f_ex_font_style\":\"undefined\",\"f_ex_font_transform\":\"undefined\",\"f_ex_font_spacing\":\"undefined\",\"excl_txt\":\"Locked\",\"excl_margin\":\"-4px 5px 0 0\",\"excl_padd\":\"eyJhbGwiOiI1cHggOHB4IiwibGFuZHNjYXBlIjoiNHB4IDZweCIsInBvcnRyYWl0IjoiM3B4IDVweCIsInBob25lIjoiNHB4IDZweCJ9\",\"excl_color_h\":\"#ffffff\",\"excl_bg\":\"var(--downtown-menu-bg-light)\",\"excl_bg_h\":\"var(--downtown-menu-bg-light)\",\"f_excl_font_family\":\"downtown-sans-serif-font_global\",\"f_excl_font_size\":\"eyJhbGwiOiIxMSIsImxhbmRzY2FwZSI6IjEwIiwicG9ydHJhaXQiOiIxMCIsInBob25lIjoiMTAifQ==\",\"f_excl_font_line_height\":\"1.1\",\"f_excl_font_transform\":\"uppercase\",\"pag_border_width\":\"0\",\"pag_space\":\"eyJhbGwiOiIyNSIsInBvcnRyYWl0IjoiMjAifQ==\",\"f_pag_font_family\":\"downtown-sans-serif-font_global\",\"f_pag_font_weight\":\"500\",\"ad_loop_full\":\"\",\"f_btn_font_family\":\"downtown-sans-serif-font_global\",\"modules_border_size\":\"1px 1px 1px 1px\",\"modules_border_style\":\"dashed\",\"modules_border_color\":\"#dddddd\",\"m_radius\":\"10px\",\"search_query\":\"{keyword}\",\"block_type\":\"tdb_loop\",\"separator\":\"\",\"custom_title\":\"\",\"custom_url\":\"\",\"block_template_id\":\"\",\"title_tag\":\"\",\"mc1_tl\":\"\",\"mc1_title_tag\":\"\",\"mc1_el\":\"\",\"offset\":\"\",\"open_in_new_window\":\"\",\"post_ids\":\"-585390\",\"include_cf_posts\":\"\",\"exclude_cf_posts\":\"\",\"sort\":\"\",\"installed_post_types\":\"\",\"ajax_pagination_next_prev_swipe\":\"\",\"ajax_pagination_infinite_stop\":\"\",\"review_source\":\"\",\"container_width\":\"\",\"m_padding\":\"\",\"modules_divider\":\"\",\"modules_divider_color\":\"#eaeaea\",\"h_effect\":\"\",\"image_size\":\"\",\"image_alignment\":\"50\",\"hide_image\":\"\",\"show_favourites\":\"\",\"fav_size\":\"2\",\"fav_space\":\"\",\"fav_ico_color\":\"\",\"fav_ico_color_h\":\"\",\"fav_bg\":\"\",\"fav_bg_h\":\"\",\"fav_shadow_shadow_header\":\"\",\"fav_shadow_shadow_title\":\"Shadow\",\"fav_shadow_shadow_size\":\"\",\"fav_shadow_shadow_offset_horizontal\":\"\",\"fav_shadow_shadow_offset_vertical\":\"\",\"fav_shadow_shadow_spread\":\"\",\"fav_shadow_shadow_color\":\"\",\"video_icon\":\"\",\"video_popup\":\"yes\",\"video_rec\":\"\",\"spot_header\":\"\",\"video_rec_title\":\"- Advertisement -\",\"video_rec_color\":\"\",\"video_rec_disable\":\"\",\"autoplay_vid\":\"yes\",\"show_vid_t\":\"block\",\"vid_t_margin\":\"\",\"vid_t_padding\":\"\",\"video_title_color\":\"\",\"video_title_color_h\":\"\",\"video_bg\":\"\",\"video_overlay\":\"\",\"vid_t_color\":\"\",\"vid_t_bg_color\":\"\",\"f_vid_title_font_header\":\"\",\"f_vid_title_font_title\":\"Video pop-up article title\",\"f_vid_title_font_settings\":\"\",\"f_vid_title_font_family\":\"\",\"f_vid_title_font_size\":\"\",\"f_vid_title_font_line_height\":\"\",\"f_vid_title_font_style\":\"\",\"f_vid_title_font_weight\":\"\",\"f_vid_title_font_transform\":\"\",\"f_vid_title_font_spacing\":\"\",\"f_vid_title_\":\"\",\"f_vid_time_font_title\":\"Video duration text\",\"f_vid_time_font_settings\":\"\",\"f_vid_time_font_family\":\"\",\"f_vid_time_font_size\":\"\",\"f_vid_time_font_line_height\":\"\",\"f_vid_time_font_style\":\"\",\"f_vid_time_font_weight\":\"\",\"f_vid_time_font_transform\":\"\",\"f_vid_time_font_spacing\":\"\",\"f_vid_time_\":\"\",\"meta_width\":\"\",\"meta_margin\":\"\",\"meta_space\":\"\",\"meta_info_border_size\":\"\",\"meta_info_border_style\":\"\",\"meta_info_border_color\":\"#eaeaea\",\"meta_info_border_radius\":\"\",\"art_btn\":\"\",\"modules_extra_cat\":\"\",\"author_photo\":\"\",\"author_photo_size\":\"\",\"author_photo_space\":\"\",\"author_photo_radius\":\"\",\"show_modified_date\":\"\",\"time_ago\":\"\",\"time_ago_add_txt\":\"ago\",\"time_ago_txt_pos\":\"\",\"review_space\":\"\",\"review_size\":\"2.5\",\"review_distance\":\"\",\"excerpt_col\":\"1\",\"excerpt_gap\":\"\",\"excerpt_middle\":\"\",\"excerpt_inline\":\"\",\"show_audio\":\"block\",\"art_audio\":\"\",\"art_audio_size\":\"1.5\",\"btn_title\":\"\",\"btn_margin\":\"\",\"btn_padding\":\"\",\"btn_border_width\":\"\",\"btn_radius\":\"\",\"pag_padding\":\"\",\"pag_border_radius\":\"\",\"prev_tdicon\":\"\",\"next_tdicon\":\"\",\"pag_icons_size\":\"\",\"f_header_font_header\":\"\",\"f_header_font_title\":\"Block header\",\"f_header_font_settings\":\"\",\"f_header_font_family\":\"\",\"f_header_font_size\":\"\",\"f_header_font_line_height\":\"\",\"f_header_font_style\":\"\",\"f_header_font_weight\":\"\",\"f_header_font_transform\":\"\",\"f_header_font_spacing\":\"\",\"f_header_\":\"\",\"f_pag_font_title\":\"Pagination text\",\"f_pag_font_settings\":\"\",\"f_pag_font_size\":\"\",\"f_pag_font_line_height\":\"\",\"f_pag_font_style\":\"\",\"f_pag_font_transform\":\"\",\"f_pag_font_spacing\":\"\",\"f_pag_\":\"\",\"f_title_font_header\":\"\",\"f_title_font_title\":\"Article title\",\"f_title_font_settings\":\"\",\"f_title_\":\"\",\"f_cat_font_title\":\"Article category tag\",\"f_cat_font_settings\":\"\",\"f_cat_\":\"\",\"f_meta_font_title\":\"Article meta info\",\"f_meta_font_settings\":\"\",\"f_meta_\":\"\",\"f_ex_font_title\":\"Article excerpt\",\"f_ex_font_settings\":\"\",\"f_ex_\":\"\",\"f_btn_font_title\":\"Article read more button\",\"f_btn_font_settings\":\"\",\"f_btn_font_size\":\"\",\"f_btn_font_line_height\":\"\",\"f_btn_font_style\":\"\",\"f_btn_font_weight\":\"\",\"f_btn_font_transform\":\"\",\"f_btn_font_spacing\":\"\",\"f_btn_\":\"\",\"mix_color\":\"\",\"mix_type\":\"\",\"fe_brightness\":\"1\",\"fe_contrast\":\"1\",\"fe_saturate\":\"1\",\"mix_color_h\":\"\",\"mix_type_h\":\"\",\"fe_brightness_h\":\"1\",\"fe_contrast_h\":\"1\",\"fe_saturate_h\":\"1\",\"m_bg\":\"\",\"shadow_shadow_header\":\"\",\"shadow_shadow_title\":\"Module Shadow\",\"shadow_shadow_size\":\"\",\"shadow_shadow_offset_horizontal\":\"\",\"shadow_shadow_offset_vertical\":\"\",\"shadow_shadow_spread\":\"\",\"shadow_shadow_color\":\"\",\"all_underline_height\":\"\",\"all_underline_color\":\"#000\",\"cat_border\":\"\",\"cat_border_hover\":\"\",\"meta_bg\":\"\",\"com_bg\":\"\",\"com_txt\":\"\",\"rev_txt\":\"\",\"shadow_m_shadow_header\":\"\",\"shadow_m_shadow_title\":\"Meta info shadow\",\"shadow_m_shadow_size\":\"\",\"shadow_m_shadow_offset_horizontal\":\"\",\"shadow_m_shadow_offset_vertical\":\"\",\"shadow_m_shadow_spread\":\"\",\"shadow_m_shadow_color\":\"\",\"audio_btn_color\":\"\",\"audio_time_color\":\"\",\"audio_bar_color\":\"\",\"audio_bar_curr_color\":\"\",\"btn_bg\":\"\",\"btn_bg_hover\":\"\",\"btn_txt\":\"\",\"btn_txt_hover\":\"\",\"btn_border\":\"\",\"btn_border_hover\":\"\",\"nextprev_border_h\":\"\",\"pag_text\":\"\",\"pag_h_text\":\"\",\"pag_a_text\":\"\",\"pag_bg\":\"\",\"pag_border\":\"\",\"pag_h_border\":\"\",\"pag_a_border\":\"\",\"ad_loop\":\"\",\"ad_loop_title\":\"- Advertisement -\",\"ad_loop_repeat\":\"\",\"ad_loop_color\":\"\",\"f_ad_font_header\":\"\",\"f_ad_font_title\":\"Ad title text\",\"f_ad_font_settings\":\"\",\"f_ad_font_family\":\"\",\"f_ad_font_size\":\"\",\"f_ad_font_line_height\":\"\",\"f_ad_font_style\":\"\",\"f_ad_font_weight\":\"\",\"f_ad_font_transform\":\"\",\"f_ad_font_spacing\":\"\",\"f_ad_\":\"\",\"ad_loop_disable\":\"\",\"el_class\":\"\",\"td_column_number\":1,\"header_color\":\"\",\"td_ajax_preloading\":\"\",\"td_ajax_filter_type\":\"\",\"td_filter_default_txt\":\"\",\"td_ajax_filter_ids\":\"\",\"color_preset\":\"\",\"border_top\":\"\",\"css\":\"\",\"class\":\"tdi_59\",\"tdc_css_class\":\"tdi_59\",\"tdc_css_class_style\":\"tdi_59_rand_style\"}}",
                "td_block_id": "tdi_59",
                "td_column_number": "1",
                "td_current_page": "1",
                "block_type": "tdb_loop",
                "td_filter_value": "",
                "td_user_action": "",
                "td_magic_token": f"{td_magic_token}"
            }
            yield feapder.Request(url, params=params, data=data, callback=self.parse_url, page=page, keyword=keyword,
                                  method="POST",td_magic_token=td_magic_token,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "referer": "https://www.omanobserver.om/search?query=heavy%20rain&pgno=2",
            "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Microsoft Edge\";v=\"132\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0"
        }
        request.cookies = {
            "device": "web",
            "Path": "/",
            "_ga": "GA1.1.1843095612.1738822372",
            "_cb": "BRwndKB71b6OB8oVXG",
            "_cb_svref": "external",
            "_chartbeat2": ".1738822372347.1738822482256.1.D4XfkbC7vcgOB35FnMDiRziTDK_Ym9.3",
            "_ga_NM3F4TH9XN": "GS1.1.1738822371.1.1.1738822511.16.0.0"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")
        html_str = response.json['td_data']
        tree = etree.HTML(html_str)
        links = tree.xpath("//h3/a/@href")

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
            items.table_name = self.table  # "https://www.dr.dk/" + item.get("urlPathId") if item.get("urlPathId") else "https://www.dr.dk/" + item.get("url")
            items.article_url = item
            # items.title = item.get("title")
            items.country = self.country
            items.keyword = request.keyword  # 确保 items.keyword 被正确赋值
            items.pubtime = ''
            items.author = ''
            yield feapder.Request(url=items.article_url, callback=self.parse_detail, items=items)

        current_page = request.page + 1
        url = "https://arynews.tv/wp-admin/admin-ajax.php"
        td_magic_token = request.td_magic_token
        params = {
            "td_theme_name": "Newspaper",
            "v": "12.6.2"
        }
        data = {
            "action": "td_ajax_block",
            "td_atts": f"{{\"modules_on_row\":\"50%\",\"modules_gap\":\"10\",\"modules_category\":\"above\",\"show_excerpt\":\"eyJwb3J0cmFpdCI6Im5vbmUiLCJhbGwiOiJub25lIn0=\",\"show_btn\":\"none\",\"ajax_pagination\":\"load_more\",\"hide_audio\":\"yes\",\"limit\":\"20\",\"image_width\":\"eyJhbGwiOiIzMCIsInBob25lIjoiMTAwIn0=\",\"image_floated\":\"eyJhbGwiOiJmbG9hdF9sZWZ0IiwicGhvbmUiOiJub19mbG9hdCJ9\",\"meta_padding\":\"eyJhbGwiOiIwIDAgMCAzMHB4IiwibGFuZHNjYXBlIjoiMCAwIDAgMjVweCIsInBvcnRyYWl0IjoiMCAwIDAgMjBweCIsInBob25lIjoiMjVweCAwIDAgMCJ9\",\"image_radius\":\"10\",\"image_height\":\"eyJwaG9uZSI6IjExMCIsImFsbCI6Ijc1In0=\",\"meta_info_horiz\":\"\",\"modules_category_margin\":\"eyJhbGwiOiIwIiwicG9ydHJhaXQiOiIwIn0=\",\"show_cat\":\"none\",\"show_author\":\"none\",\"show_com\":\"none\",\"show_review\":\"none\",\"show_date\":\"none\",\"art_title\":\"eyJhbGwiOiIxMHB4IDAiLCJwb3J0cmFpdCI6IjZweCAwIiwibGFuZHNjYXBlIjoiOHB4IDAifQ==\",\"f_title_font_family\":\"downtown-sans-serif-font_global\",\"f_title_font_size\":\"eyJhbGwiOiIyMiIsImxhbmRzY2FwZSI6IjI4IiwicG9ydHJhaXQiOiIyNCIsInBob25lIjoiMzIifQ==\",\"f_title_font_line_height\":\"1\",\"f_title_font_weight\":\"900\",\"f_title_font_transform\":\"undefined\",\"title_txt\":\"#000000\",\"title_txt_hover\":\"#444444\",\"modules_category_padding\":\"eyJhbGwiOiIwIiwicG9ydHJhaXQiOiIwIn0=\",\"f_cat_font_family\":\"downtown-sans-serif-font_global\",\"f_cat_font_size\":\"eyJhbGwiOiIxNSIsImxhbmRzY2FwZSI6IjE0IiwicG9ydHJhaXQiOiIxMyJ9\",\"f_cat_font_line_height\":\"1\",\"f_cat_font_weight\":\"700\",\"f_cat_font_transform\":\"\",\"cat_bg\":\"rgba(255,255,255,0)\",\"cat_bg_hover\":\"rgba(255,255,255,0)\",\"cat_txt\":\"#edb500\",\"author_txt\":\"undefined\",\"author_txt_hover\":\"undefined\",\"art_excerpt\":\"eyJwb3J0cmFpdCI6IjZweCAwIDAgMCIsImFsbCI6IjAifQ==\",\"f_ex_font_family\":\"downtown-sans-serif-font_global\",\"f_ex_font_size\":\"eyJhbGwiOiIxNSIsImxhbmRzY2FwZSI6IjE0IiwicG9ydHJhaXQiOiIxMyJ9\",\"f_ex_font_line_height\":\"1.4\",\"f_ex_font_weight\":\"500\",\"ex_txt\":\"#666666\",\"meta_info_align\":\"center\",\"all_modules_space\":\"eyJhbGwiOiIyMCIsInBvcnRyYWl0IjoiMTAiLCJsYW5kc2NhcGUiOiIxNSIsInBob25lIjoiMzAifQ==\",\"tdc_css\":\"eyJhbGwiOnsibWFyZ2luLWJvdHRvbSI6IjAiLCJkaXNwbGF5IjoiIn0sImxhbmRzY2FwZSI6eyJkaXNwbGF5IjoiIn0sImxhbmRzY2FwZV9tYXhfd2lkdGgiOjExNDAsImxhbmRzY2FwZV9taW5fd2lkdGgiOjEwMTksInBvcnRyYWl0Ijp7ImRpc3BsYXkiOiIifSwicG9ydHJhaXRfbWF4X3dpZHRoIjoxMDE4LCJwb3J0cmFpdF9taW5fd2lkdGgiOjc2OCwicGhvbmUiOnsiZGlzcGxheSI6IiJ9LCJwaG9uZV9tYXhfd2lkdGgiOjc2N30=\",\"pag_h_bg\":\"var(--downtown-menu-bg)\",\"pag_a_bg\":\"var(--downtown-menu-bg)\",\"modules_cat_border\":\"0\",\"modules_category_radius\":\"50\",\"f_cat_font_style\":\"undefined\",\"f_cat_font_spacing\":\"undefined\",\"cat_txt_hover\":\"#000000\",\"f_title_font_style\":\"undefined\",\"f_title_font_spacing\":\"undefined\",\"f_meta_font_family\":\"downtown-sans-serif-font_global\",\"f_meta_font_size\":\"eyJhbGwiOiIxMyIsInBvcnRyYWl0IjoiMTIifQ==\",\"f_meta_font_line_height\":\"1\",\"f_meta_font_style\":\"undefined\",\"f_meta_font_weight\":\"500\",\"f_meta_font_transform\":\"capitalize\",\"f_meta_font_spacing\":\"undefined\",\"date_txt\":\"#666666\",\"f_ex_font_style\":\"undefined\",\"f_ex_font_transform\":\"undefined\",\"f_ex_font_spacing\":\"undefined\",\"excl_txt\":\"Locked\",\"excl_margin\":\"-4px 5px 0 0\",\"excl_padd\":\"eyJhbGwiOiI1cHggOHB4IiwibGFuZHNjYXBlIjoiNHB4IDZweCIsInBvcnRyYWl0IjoiM3B4IDVweCIsInBob25lIjoiNHB4IDZweCJ9\",\"excl_color_h\":\"#ffffff\",\"excl_bg\":\"var(--downtown-menu-bg-light)\",\"excl_bg_h\":\"var(--downtown-menu-bg-light)\",\"f_excl_font_family\":\"downtown-sans-serif-font_global\",\"f_excl_font_size\":\"eyJhbGwiOiIxMSIsImxhbmRzY2FwZSI6IjEwIiwicG9ydHJhaXQiOiIxMCIsInBob25lIjoiMTAifQ==\",\"f_excl_font_line_height\":\"1.1\",\"f_excl_font_transform\":\"uppercase\",\"pag_border_width\":\"0\",\"pag_space\":\"eyJhbGwiOiIyNSIsInBvcnRyYWl0IjoiMjAifQ==\",\"f_pag_font_family\":\"downtown-sans-serif-font_global\",\"f_pag_font_weight\":\"500\",\"ad_loop_full\":\"\",\"f_btn_font_family\":\"downtown-sans-serif-font_global\",\"modules_border_size\":\"1px 1px 1px 1px\",\"modules_border_style\":\"dashed\",\"modules_border_color\":\"#dddddd\",\"m_radius\":\"10px\",\"search_query\":\"{current_keyword}\",\"block_type\":\"tdb_loop\",\"separator\":\"\",\"custom_title\":\"\",\"custom_url\":\"\",\"block_template_id\":\"\",\"title_tag\":\"\",\"mc1_tl\":\"\",\"mc1_title_tag\":\"\",\"mc1_el\":\"\",\"offset\":\"\",\"open_in_new_window\":\"\",\"post_ids\":\"-585390\",\"include_cf_posts\":\"\",\"exclude_cf_posts\":\"\",\"sort\":\"\",\"installed_post_types\":\"\",\"ajax_pagination_next_prev_swipe\":\"\",\"ajax_pagination_infinite_stop\":\"\",\"review_source\":\"\",\"container_width\":\"\",\"m_padding\":\"\",\"modules_divider\":\"\",\"modules_divider_color\":\"#eaeaea\",\"h_effect\":\"\",\"image_size\":\"\",\"image_alignment\":\"50\",\"hide_image\":\"\",\"show_favourites\":\"\",\"fav_size\":\"2\",\"fav_space\":\"\",\"fav_ico_color\":\"\",\"fav_ico_color_h\":\"\",\"fav_bg\":\"\",\"fav_bg_h\":\"\",\"fav_shadow_shadow_header\":\"\",\"fav_shadow_shadow_title\":\"Shadow\",\"fav_shadow_shadow_size\":\"\",\"fav_shadow_shadow_offset_horizontal\":\"\",\"fav_shadow_shadow_offset_vertical\":\"\",\"fav_shadow_shadow_spread\":\"\",\"fav_shadow_shadow_color\":\"\",\"video_icon\":\"\",\"video_popup\":\"yes\",\"video_rec\":\"\",\"spot_header\":\"\",\"video_rec_title\":\"- Advertisement -\",\"video_rec_color\":\"\",\"video_rec_disable\":\"\",\"autoplay_vid\":\"yes\",\"show_vid_t\":\"block\",\"vid_t_margin\":\"\",\"vid_t_padding\":\"\",\"video_title_color\":\"\",\"video_title_color_h\":\"\",\"video_bg\":\"\",\"video_overlay\":\"\",\"vid_t_color\":\"\",\"vid_t_bg_color\":\"\",\"f_vid_title_font_header\":\"\",\"f_vid_title_font_title\":\"Video pop-up article title\",\"f_vid_title_font_settings\":\"\",\"f_vid_title_font_family\":\"\",\"f_vid_title_font_size\":\"\",\"f_vid_title_font_line_height\":\"\",\"f_vid_title_font_style\":\"\",\"f_vid_title_font_weight\":\"\",\"f_vid_title_font_transform\":\"\",\"f_vid_title_font_spacing\":\"\",\"f_vid_title_\":\"\",\"f_vid_time_font_title\":\"Video duration text\",\"f_vid_time_font_settings\":\"\",\"f_vid_time_font_family\":\"\",\"f_vid_time_font_size\":\"\",\"f_vid_time_font_line_height\":\"\",\"f_vid_time_font_style\":\"\",\"f_vid_time_font_weight\":\"\",\"f_vid_time_font_transform\":\"\",\"f_vid_time_font_spacing\":\"\",\"f_vid_time_\":\"\",\"meta_width\":\"\",\"meta_margin\":\"\",\"meta_space\":\"\",\"meta_info_border_size\":\"\",\"meta_info_border_style\":\"\",\"meta_info_border_color\":\"#eaeaea\",\"meta_info_border_radius\":\"\",\"art_btn\":\"\",\"modules_extra_cat\":\"\",\"author_photo\":\"\",\"author_photo_size\":\"\",\"author_photo_space\":\"\",\"author_photo_radius\":\"\",\"show_modified_date\":\"\",\"time_ago\":\"\",\"time_ago_add_txt\":\"ago\",\"time_ago_txt_pos\":\"\",\"review_space\":\"\",\"review_size\":\"2.5\",\"review_distance\":\"\",\"excerpt_col\":\"1\",\"excerpt_gap\":\"\",\"excerpt_middle\":\"\",\"excerpt_inline\":\"\",\"show_audio\":\"block\",\"art_audio\":\"\",\"art_audio_size\":\"1.5\",\"btn_title\":\"\",\"btn_margin\":\"\",\"btn_padding\":\"\",\"btn_border_width\":\"\",\"btn_radius\":\"\",\"pag_padding\":\"\",\"pag_border_radius\":\"\",\"prev_tdicon\":\"\",\"next_tdicon\":\"\",\"pag_icons_size\":\"\",\"f_header_font_header\":\"\",\"f_header_font_title\":\"Block header\",\"f_header_font_settings\":\"\",\"f_header_font_family\":\"\",\"f_header_font_size\":\"\",\"f_header_font_line_height\":\"\",\"f_header_font_style\":\"\",\"f_header_font_weight\":\"\",\"f_header_font_transform\":\"\",\"f_header_font_spacing\":\"\",\"f_header_\":\"\",\"f_pag_font_title\":\"Pagination text\",\"f_pag_font_settings\":\"\",\"f_pag_font_size\":\"\",\"f_pag_font_line_height\":\"\",\"f_pag_font_style\":\"\",\"f_pag_font_transform\":\"\",\"f_pag_font_spacing\":\"\",\"f_pag_\":\"\",\"f_title_font_header\":\"\",\"f_title_font_title\":\"Article title\",\"f_title_font_settings\":\"\",\"f_title_\":\"\",\"f_cat_font_title\":\"Article category tag\",\"f_cat_font_settings\":\"\",\"f_cat_\":\"\",\"f_meta_font_title\":\"Article meta info\",\"f_meta_font_settings\":\"\",\"f_meta_\":\"\",\"f_ex_font_title\":\"Article excerpt\",\"f_ex_font_settings\":\"\",\"f_ex_\":\"\",\"f_btn_font_title\":\"Article read more button\",\"f_btn_font_settings\":\"\",\"f_btn_font_size\":\"\",\"f_btn_font_line_height\":\"\",\"f_btn_font_style\":\"\",\"f_btn_font_weight\":\"\",\"f_btn_font_transform\":\"\",\"f_btn_font_spacing\":\"\",\"f_btn_\":\"\",\"mix_color\":\"\",\"mix_type\":\"\",\"fe_brightness\":\"1\",\"fe_contrast\":\"1\",\"fe_saturate\":\"1\",\"mix_color_h\":\"\",\"mix_type_h\":\"\",\"fe_brightness_h\":\"1\",\"fe_contrast_h\":\"1\",\"fe_saturate_h\":\"1\",\"m_bg\":\"\",\"shadow_shadow_header\":\"\",\"shadow_shadow_title\":\"Module Shadow\",\"shadow_shadow_size\":\"\",\"shadow_shadow_offset_horizontal\":\"\",\"shadow_shadow_offset_vertical\":\"\",\"shadow_shadow_spread\":\"\",\"shadow_shadow_color\":\"\",\"all_underline_height\":\"\",\"all_underline_color\":\"#000\",\"cat_border\":\"\",\"cat_border_hover\":\"\",\"meta_bg\":\"\",\"com_bg\":\"\",\"com_txt\":\"\",\"rev_txt\":\"\",\"shadow_m_shadow_header\":\"\",\"shadow_m_shadow_title\":\"Meta info shadow\",\"shadow_m_shadow_size\":\"\",\"shadow_m_shadow_offset_horizontal\":\"\",\"shadow_m_shadow_offset_vertical\":\"\",\"shadow_m_shadow_spread\":\"\",\"shadow_m_shadow_color\":\"\",\"audio_btn_color\":\"\",\"audio_time_color\":\"\",\"audio_bar_color\":\"\",\"audio_bar_curr_color\":\"\",\"btn_bg\":\"\",\"btn_bg_hover\":\"\",\"btn_txt\":\"\",\"btn_txt_hover\":\"\",\"btn_border\":\"\",\"btn_border_hover\":\"\",\"nextprev_border_h\":\"\",\"pag_text\":\"\",\"pag_h_text\":\"\",\"pag_a_text\":\"\",\"pag_bg\":\"\",\"pag_border\":\"\",\"pag_h_border\":\"\",\"pag_a_border\":\"\",\"ad_loop\":\"\",\"ad_loop_title\":\"- Advertisement -\",\"ad_loop_repeat\":\"\",\"ad_loop_color\":\"\",\"f_ad_font_header\":\"\",\"f_ad_font_title\":\"Ad title text\",\"f_ad_font_settings\":\"\",\"f_ad_font_family\":\"\",\"f_ad_font_size\":\"\",\"f_ad_font_line_height\":\"\",\"f_ad_font_style\":\"\",\"f_ad_font_weight\":\"\",\"f_ad_font_transform\":\"\",\"f_ad_font_spacing\":\"\",\"f_ad_\":\"\",\"ad_loop_disable\":\"\",\"el_class\":\"\",\"td_column_number\":1,\"header_color\":\"\",\"td_ajax_preloading\":\"\",\"td_ajax_filter_type\":\"\",\"td_filter_default_txt\":\"\",\"td_ajax_filter_ids\":\"\",\"color_preset\":\"\",\"border_top\":\"\",\"css\":\"\",\"class\":\"tdi_59\",\"tdc_css_class\":\"tdi_59\",\"tdc_css_class_style\":\"tdi_59_rand_style\"}}",
            "td_block_id": "tdi_59",
            "td_column_number": "1",
            "td_current_page": f"{current_page}",
            "block_type": "tdb_loop",
            "td_filter_value": "",
            "td_user_action": "",
            "td_magic_token": f"{td_magic_token}"
        }
        yield feapder.Request(url, params=params, data=data, callback=self.parse_url, page=current_page,
                              keyword=current_keyword, method='POST',td_magic_token=td_magic_token,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='tdb-block-inner td-fix-index']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//meta[@property='article:published_time']/@content").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
