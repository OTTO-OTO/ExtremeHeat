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
    sql = """ select db_name from keywords where country='Poland' and language='波兰语'"""
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

    country = 'Poland'
    table = 'Poland'
    #波兰语
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
    keywords = db.find(f"select keywords_list from keywords where language = '波兰语' and country='{table}'", to_json=True)[0].get(
        "keywords_list")
    # keywords = ["Wysoka temperatura"]
    print("待抓取的关键词列===========>", keywords)
    def start_requests(self):
        for keyword in self.keywords:
            page = 1
            url = "https://www.polsatnews.pl/wyszukiwarka/"
            params = {
                "text": f"{keyword}",
                "page": f"1"
            }
            yield feapder.Request(url, params=params, callback=self.parse_url, page=page, method='GET',
                                  keyword=keyword,
                                  filter_repeat=True)

    def download_midware(self, request):
        request.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "priority": "u=0, i",
            "referer": "https://www.polsatnews.pl/wyszukiwarka/?text=Wysoka+temperatura",
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
        request.cookies = {
            "_ga": "GA1.1.1537003669.1742263628",
            "__gfp_64b": "bbCKpugVc2X1weut8uibdcUb2sp5YtJqdEGv3O5iMJn.G7|1742263599|2|ruid.hit|8|8:1:80",
            "__iwa_dvid": "cc3a5923-406b-473c-9b6a-dc0d2a8d835d$IWAStorageItem$",
            "__gfp_ruid": "x73HRf7m.7AQf9VNFcc2YVlktkOc_cnovP0zjzo7sU..l7|1742350030|0",
            "OptanonAlertBoxClosed": "2025-03-18T02:07:12.804Z",
            "eupubconsent-v2": "CQOcsYAQOcsYAAcABBPLBhFsAP_gAEPgAAYgKENV_G__bWlr8X73aftkeY1P99h77sQxBgbJE-4FzLuW_JwXx2ExNAz6tqIKmRIAu3TBIQNlGJDURVCgaogVrSDMaECUgTNKJ6BkiFMRI2dYCFxvmwtjeQCY5vp991dx2Bet7dr83dzyy4BHn3a5_2S1WJCdAYctDfv9bROb-9IOd_x8v4vw_F7pE2-eS1l_tWvp7D8-Yts_9XW99_bbfb5PnBQgAkw0KiCMsiQkIlAwggQAqCsICKBAEAACQNEBACYMCnIGAC6wkQAgBQADBACAAEGAAIAABIAEIgAoAKBAABAIFAAAAAAIBAAwMAAYALAQCAAEB0CFMCCAQLABIzIoFMCAABIICWyoQSAIEFcIQizwCABETBQAAAgAFIAAgPBYDEkgJWJBAFxBNAAAQAABBAAUIpOzAEEAZstReDJ9GVpAWD4AAAAA.f_wACHwAAAAA",
            "_ga_Z60VXZ6NV5": "GS1.1.1742263628.1.1.1742263745.60.0.0",
            "OptanonConsent": "isGpcEnabled=0&datestamp=Tue+Mar+18+2025+10%3A09%3A05+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202502.1.0&browserGpcFlag=0&isIABGlobal=false&consentId=33d2b3c3-ec0b-4664-9628-8c6c215b599a&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0004%3A1%2CC0003%3A1%2CC0002%3A1%2CV2STACK42%3A1&hosts=H1498%3A1%2CH677%3A1%2CH37%3A1%2CH814%3A1%2CH138%3A1%2CH1585%3A1%2CH204%3A1%2CH793%3A1%2CH139%3A1%2CH140%3A1%2CH141%3A1%2CH1714%3A1%2CH1586%3A1%2CH1587%3A1%2CH142%3A1%2CH1588%3A1%2CH1762%3A1%2CH1589%3A1%2CH1793%3A1%2CH1715%3A1%2CH40%3A1%2CH1%3A1%2CH1590%3A1%2CH1591%3A1%2CH2%3A1%2CH1801%3A1%2CH1592%3A1%2CH1593%3A1%2CH240%3A1%2CH1594%3A1%2CH1716%3A1%2CH1717%3A1%2CH1718%3A1%2CH1595%3A1%2CH205%3A1%2CH1596%3A1%2CH1719%3A1%2CH1597%3A1%2CH1598%3A1%2CH1720%3A1%2CH1599%3A1%2CH1600%3A1%2CH1601%3A1%2CH1602%3A1%2CH5%3A1%2CH206%3A1%2CH796%3A1%2CH1721%3A1%2CH1603%3A1%2CH1604%3A1%2CH143%3A1%2CH1605%3A1%2CH1606%3A1%2CH797%3A1%2CH241%3A1%2CH1763%3A1%2CH144%3A1%2CH6%3A1%2CH1607%3A1%2CH1608%3A1%2CH1609%3A1%2CH1610%3A1%2CH1611%3A1%2CH1612%3A1%2CH1613%3A1%2CH1614%3A1%2CH1615%3A1%2CH1616%3A1%2CH1617%3A1%2CH41%3A1%2CH817%3A1%2CH1618%3A1%2CH1722%3A1%2CH1619%3A1%2CH1723%3A1%2CH1620%3A1%2CH1512%3A1%2CH1621%3A1%2CH79%3A1%2CH1622%3A1%2CH1764%3A1%2CH1623%3A1%2CH1724%3A1%2CH1624%3A1%2CH1625%3A1%2CH10%3A1%2CH1626%3A1%2CH1534%3A1%2CH1627%3A1%2CH43%3A1%2CH1628%3A1%2CH798%3A1%2CH432%3A1%2CH436%3A1%2CH208%3A1%2CH146%3A1%2CH1629%3A1%2CH457%3A1%2CH242%3A1%2CH1630%3A1%2CH818%3A1%2CH799%3A1%2CH258%3A1%2CH1631%3A1%2CH46%3A1%2CH1632%3A1%2CH1752%3A1%2CH1725%3A1%2CH1726%3A1%2CH1633%3A1%2CH47%3A1%2CH1634%3A1%2CH13%3A1%2CH1635%3A1%2CH1636%3A1%2CH49%3A1%2CH50%3A1%2CH1637%3A1%2CH209%3A1%2CH1638%3A1%2CH15%3A1%2CH1639%3A1%2CH1727%3A1%2CH1640%3A1%2CH1813%3A1%2CH1728%3A1%2CH1803%3A1%2CH1798%3A1%2CH1641%3A1%2CH210%3A1%2CH1642%3A1%2CH1643%3A1%2CH16%3A1%2CH1729%3A1%2CH1644%3A1%2CH51%3A1%2CH510%3A1%2CH1730%3A1%2CH1754%3A1%2CH147%3A1%2CH52%3A1%2CH800%3A1%2CH211%3A1%2CH1646%3A1%2CH148%3A1%2CH1755%3A1%2CH1647%3A1%2CH802%3A1%2CH1648%3A1%2CH1649%3A1%2CH1650%3A1%2CH81%3A1%2CH527%3A1%2CH1549%3A1%2CH225%3A1%2CH1783%3A1%2CH1550%3A1%2CH1651%3A1%2CH543%3A1%2CH1652%3A1%2CH1653%3A1%2CH1583%3A1%2CH1731%3A1%2CH54%3A1%2CH1654%3A1%2CH55%3A1%2CH248%3A1%2CH19%3A1%2CH1655%3A1%2CH1656%3A1%2CH1757%3A1%2CH1732%3A1%2CH94%3A1%2CH56%3A1%2CH1733%3A1%2CH1657%3A1%2CH21%3A1%2CH1658%3A1%2CH1734%3A1%2CH1659%3A1%2CH1660%3A1%2CH1735%3A1%2CH1661%3A1%2CH1662%3A1%2CH59%3A1%2CH1663%3A1%2CH1664%3A1%2CH806%3A1%2CH1501%3A1%2CH1665%3A1%2CH1666%3A1%2CH1736%3A1%2CH1667%3A1%2CH63%3A1%2CH1737%3A1%2CH1738%3A1%2CH610%3A1%2CH1668%3A1%2CH1669%3A1%2CH151%3A1%2CH65%3A1%2CH618%3A1%2CH1670%3A1%2CH25%3A1%2CH807%3A1%2CH1671%3A1%2CH152%3A1%2CH1672%3A1%2CH1673%3A1%2CH634%3A1%2CH1674%3A1%2CH1675%3A1%2CH1676%3A1%2CH66%3A1%2CH1739%3A1%2CH67%3A1%2CH153%3A1%2CH1677%3A1%2CH1678%3A1%2CH808%3A1%2CH1679%3A1%2CH1680%3A1%2CH1681%3A1%2CH282%3A1%2CH1682%3A1%2CH27%3A1%2CH1683%3A1%2CH1740%3A1%2CH253%3A1%2CH28%3A1%2CH1741%3A1%2CH1742%3A1%2CH1537%3A1%2CH1684%3A1%2CH155%3A1%2CH1685%3A1%2CH156%3A1%2CH687%3A1%2CH70%3A1%2CH1743%3A1%2CH1686%3A1%2CH825%3A1%2CH1687%3A1%2CH283%3A1%2CH71%3A1%2CH1688%3A1%2CH1689%3A1%2CH190%3A1%2CH1690%3A1%2CH1691%3A1%2CH160%3A1%2CH1759%3A1%2CH1692%3A1%2CH1693%3A1%2CH1694%3A1%2CH1505%3A1%2CH1695%3A1%2CH1696%3A1%2CH73%3A1%2CH1744%3A1%2CH1697%3A1%2CH1745%3A1%2CH1746%3A1%2CH1747%3A1%2CH1698%3A1%2CH1699%3A1%2CH1563%3A1%2CH90%3A1%2CH1700%3A1%2CH32%3A1%2CH1701%3A1%2CH1702%3A1%2CH826%3A1%2CH1748%3A1%2CH1703%3A1%2CH1704%3A1%2CH1749%3A1%2CH1705%3A1%2CH1706%3A1%2CH1707%3A1%2CH1761%3A1%2CH1708%3A1%2CH34%3A1%2CH1709%3A1%2CH1710%3A1%2CH1711%3A1%2CH97%3A1%2CH1712%3A1%2CH1750%3A1%2CH1713%3A1%2CH418%3A1%2CH92%3A1%2CH831%3A1%2CH89%3A1%2CH1760%3A1%2CH33%3A1%2CH1564%3A1%2CH1565%3A1&genVendors=V1%3A1%2C&intType=1&geolocation=CN%3B&AwaitingReconsent=false"
        }
        return request

    def parse_url(self, request, response):
        current_keyword = request.keyword
        print(f"当前关键词{current_keyword}的页数为:{request.page}")

        links = response.xpath("//article/a/@href").extract()
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
        url = "https://www.polsatnews.pl/wyszukiwarka/"
        params = {
            "text": f"{current_keyword}",
            "page": f"{current_page}"
        }
        yield feapder.Request(url, params=params, callback=self.parse_url, page=current_page, method='GET',
                              keyword=current_keyword,
                              filter_repeat=True)

    def parse_detail(self, request, response):
        items = request.items
        items.title = response.xpath("//h1/text()").extract_first()
        items.content = "".join(response.xpath("//div[@class='news__content']//p/text()").extract())
        items.author = ''
        items.pubtime = response.xpath("//time/@datetime").extract_first()
        print(items)
        if items.content:
            yield items


if __name__ == "__main__":
    AirSpiderDemo().start()
