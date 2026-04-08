
from feapder import Item

class SpiderDataItem(Item):

    __unique_key__ = ["title", "article_url"] # 指定去重的key为 title、url，最后的指纹为title与url值联合计算的md5

    def __init__(self, *args, **kwargs):
        # self.id = None
        self.title = None
        self.author = None
        self.keyword = None
        self.content = None
        self.article_url = None
        self.pubtime = None
        self.country = None
        # 不要设置table_name为None，feapder内部会处理