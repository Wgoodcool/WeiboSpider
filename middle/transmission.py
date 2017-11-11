# -*- coding: utf-8 -*-
# =============================================================================
# @param url 网址
# @param category 类别
#   == 0 表明该Request为初步的user info，spider需要将其放入temeQueue队列和user数据库中
#   == 1 表明该Request为用于剩余详细信息，spider更新user数据库
#   == 2 表明该Request为粉丝数据，spider存入fans数据库
#   == 3 表明该Request为关注者数据，spider存入follow数据库
#   == 4 表明该Request为微博详情数，spider存入mblog数据库
# times 用于确定失败次数，超过一定失败次数的请求直接丢除
# =============================================================================
class Request:
    def __init__(self, url, category, meta = {}):
        self.__url = url
        self.___category = category
        self.__meta = meta
        self.__times = 0

    @property
    def url(self):
        return self.__url

    @property
    def category(self):
        return self.___category

    @property
    def meta(self):
        return self.__meta

    @property
    def time(self):
        return self.__times

    @time.setter
    def time(self, num):
        self.__times += num

class Response:
    def __init__(self, url, category, text, meta = {}):
        self.__url = url
        self.___category = category
        self.__text = text
        self.__meta = meta
    
    @property
    def meta(self):
        return self.__meta
    
    @property
    def url(self):
        return self.__url

    @property
    def text(self):
        return self.__text

    @property
    def category(self):
        return self.___category
