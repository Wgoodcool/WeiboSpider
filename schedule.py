# -*- coding: utf-8 -*-
from middle.transmission import Request
from middle.middlequeue import requestQueue, uidQueue
from manager import InfoManager

from math import ceil
import random
import time

class Schedule:
    def __init__(self):
        self.uidQueue = uidQueue
        self.requestQueue = requestQueue
# =============================================================================
# TODO: getId()可以控制一次返回的数量，不用频繁的读写数据库
# =============================================================================
#种子用户需稍微多些，以免不够用
    def Start(self):
        self.manager = InfoManager()
        self.manager.start()
        self.db_user = self.manager.UserOpe()

        self.initUrl()

        info = self.db_user.getInfo()
        print ('Is Schedule')
        while True:
            if info:
                self.CreateInfoRequest(info)
                uid = self.db_user.getId(10)
                self.CreateUidRequest(uid)
                info = self.db_user.getInfo()
            else:
                print ('sleep')
                time.sleep(random.randint(10, 15))
                uid = self.db_user.getId(5)
                self.CreateUidRequest(uid)

    def CreateUidRequest(self, uset):
        print ('CreateUidRequest')
        for n in uset:
            req = Request(self.user_url.format(n), 0, meta = {'uid' : n})
            self.uidQueue.put(req)

    def initUrl(self):
        self.user_url = 'https://m.weibo.cn/api/container/getIndex?type=uid&value={0}&containerid=100505{0}'
        self.detail_url = 'https://m.weibo.cn/api/container/getIndex?containerid=230283{0}_-_INFO&title=%25E5%259F%25BA%25E6%259C%25AC%25E4%25BF%25A1%25E6%2581%25AF&luicode=10000011&lfid=230283{0}&type=uid&value={0}'
        self.fans_url = 'https://m.weibo.cn/api/container/getIndex?containerid=231051_-_fans_-_{0}&luicode=10000011&lfid=100505{0}&featurecode=10000326&type=uid&value={0}&since_id=%s'
        self.fol_url = 'https://m.weibo.cn/api/container/getIndex?containerid=231051_-_followers_-_{0}&luicode=10000011&lfid=100505{0}&featurecode=10000326&type=uid&value={0}&page=%s'
        self.blog_url = 'https://m.weibo.cn/api/container/getIndex?uid={0}&luicode=10000011&lfid=107603{0}&featurecode=10000326&type=uid&value={0}&containerid=107603{0}&page=%s'

# 具体页码规则数还需完善
    def fillPage(self, info):
        self.uid = info[0]
        self.pageOfmblog = ceil(info[1] / 10)
        self.pageOffans = ceil(info[2] / 20)
        self.pageOffol = ceil(info[3] / 30)

    def CreateInfoRequest(self, info):
        self.fillPage(info)
        fa_url = self.fans_url.format(self.uid)
        fo_url = self.fol_url.format(self.uid)
        mb_url = self.blog_url.format(self.uid)

#   将各类请求随机取段，封装成request，加入requestQueue, 随机取段算法可能有误
        blog_start = fan_start = fol_start = 1
        pageSum = self.pageOffans + self.pageOffol + self.pageOfmblog
        while (blog_start + fan_start + fol_start <= pageSum):
            mb = blog_start < self.pageOfmblog and (random.randint(3, 5) + blog_start) or blog_start
            fa = fan_start < self.pageOffans and (random.randint(3, 5) + fan_start) or fan_start
            fo = fol_start < self.pageOffol and (random.randint(3, 5) + fol_start) or fol_start
            mb = (mb > self.pageOfmblog) and self.pageOfmblog or mb
            fa = (fa > self.pageOffans) and self.pageOffol or fa
            fo = (fa > self.pageOffol) and self.pageOffol or fo

            for n in range(fan_start, self.pageOffans + 1):
                if n == fa:
                    blog_start = fa
                    break
                req = Request((mb_url % n), 
                                category = 2, meta = {'uid' : self.uid})
                self.requestQueue.put(req)
            for n in range(fol_start, self.pageOffol + 1):
                if n == fo:
                    blog_start = fo
                    break
                req = Request((fo_url % n), 
                                category = 3, meta = {'uid' : self.uid})
                self.requestQueue.put(req)
            for n in range(blog_start, self.pageOfmblog + 1):
                if n == mb:
                    blog_start = mb
                    break
                req = Request((fa_url % n), category = 4)
                self.requestQueue.put(req)                    
        req = Request(self.detail_url.format(self.uid), category = 1)
        self.requestQueue.put(req)       

if __name__ == "__main__":
    dl = Schedule()
    dl.Start()
