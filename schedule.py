# -*- coding: utf-8 -*-
from middle.transmission import Request
from middle.queue import requestQueue, uidQueue
from database import UserOpe
from math import ceil
import random
from multiprocessing import Queue

class Schedule:
    def __init__(self):
        self.infoQueueSize = 30
        self.pageOfmblog = self.pageOffans = self.pageOffol = self.uid = 0
        self.user = UserOpe()
        self.userInfoQueue = Queue(self.infoQueueSize)

        self.user_url = 'https://m.weibo.cn/api/container/getIndex?type=uid&value={0}&containerid=100505{0}'
        self.detail_url = 'https://m.weibo.cn/api/container/getIndex?containerid=230283{0}_-_INFO&title=%25E5%259F%25BA%25E6%259C%25AC%25E4%25BF%25A1%25E6%2581%25AF&luicode=10000011&lfid=230283{0}&type=uid&value={0}'
        self.fans_url = 'https://m.weibo.cn/api/container/getIndex?containerid=231051_-_fans_-_%s&luicode=10000011&lfid=100505%s&featurecode=10000326&type=uid&value=%s&since_id={1}'
        self.fol_url = 'https://m.weibo.cn/api/container/getIndex?containerid=231051_-_followers_-_%s&luicode=10000011&lfid=100505%s&featurecode=10000326&type=uid&value=%s&page={1}'
        self.blog_url = 'https://m.weibo.cn/api/container/getIndex?uid=%s&luicode=10000011&lfid=107603%s&featurecode=10000326&type=uid&value=%s&containerid=107603%s&page={1}'

#种子用户需稍微多些，以免不够用
    def Start(self, test):
        print (test)
        info = self.user.getInfo()
        while info:
            self.CreateInfoRequest(info)
            for n in range(5):
                uid = self.user.getId()
                self.CreateUidRequest(uid)
            info = self.user.getInfo()

    def CreateUidRequest(self):
        req = Request(self.user_url.format(self.uid), 0, meta = {'uid' : self.uid})
        uidQueue.put(req)

# 具体页码规则数还需完善
    def fillPage(self, info):
        self.uid = info[0]
        self.pageOfmblog = ceil(info[1] / 10)
        self.pageOffans = ceil(info[2] / 20)
        self.pageOffol = ceil(info[3] / 30)

    def CreateInfoRequest(self, info):
        self.fillPage(info)
        fa_url = self.fans_url % self.uid
        fo_url = self.fol_url % self.uid
        mb_url = self.blog_url % self.uid

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
                req = Request(mb_url.format(n), 
                                category = 2, meta = {'uid' : self.uid})
                requestQueue.put(req)
            for n in range(fol_start, self.pageOffol + 1):
                if n == fo:
                    blog_start = fo
                    break
                req = Request(fo_url.format(n), 
                                category = 3, meta = {'uid' : self.uid})
                requestQueue.put(req)
            for n in range(blog_start, self.pageOfmblog + 1):
                if n == mb:
                    blog_start = mb
                    break
                req = Request(fa_url.format(n), category = 4)
                requestQueue.put(req)                    
        req = Request(self.detail_url.format(self.uid), category = 1)
        requestQueue.put(req)       
