# -*- coding: utf-8 -*-
from middle.transmission import Request
from middle.middlequeue import requestQueue, uidQueue
from middle.middlequeue import logQueue, errorQueue
from middle.settings import MaxrequestQueueSize
from database import UserOpe
from middle.settings import GetIDNumber, MaxErrorQueueSize

from math import ceil
import random
import time
import logging
from logging.handlers import QueueHandler

class Schedule:
    def __init__(self):
        self.uidQueue = uidQueue
        self.errorQueue = errorQueue
        self.requestQueue = requestQueue
        self.initUrl()
        #日志队列
        self.logqueue = logQueue

    def getLog(self, name):
        logger = logging.getLogger(name)
        queue_handler = QueueHandler(self.logqueue)
        logger.addHandler(queue_handler)

        return logger

#种子用户需稍微多些，以免不够用
    def Start(self):
        #manager不能放在__init__初始化？？？？
        # self.manager = InfoManager()
        # self.manager.start()
        # self.db_user = self.manager.UserOpe()
        self.db_user = UserOpe()
        info = self.db_user.getInfo()

        self.log = self.getLog('Schedule')
        self.log.warning('Start')
        try:
            while True:
                self.log.warning('Get info ' + str(info))
                if info:
                    self.CreateInfoRequest(info, self.log)
                    uid = self.db_user.getId(GetIDNumber)
                    self.CreateUidRequest(uid, self.log)
                    self.log.warning('Finish ' + str(info[0]))
                else:
                    self.log.warning('Sleep')
                    #等待时间
                    time.sleep(2)
                    uid = self.db_user.getId(GetIDNumber)
                    self.CreateUidRequest(uid, self.log)
                info = self.db_user.getInfo()

        except KeyboardInterrupt as ki:
            self.log.warning('self.requestQueue.qsize() = ' + self.requestQueue.qsize())
            self.log.warning('self.uidQueue.qsize() = ' + self.uidQueue.qsize())

    def CreateUidRequest(self, uset, log):
        for n in uset:
            req = Request(self.user_url.format(n), 0, meta = {'uid' : n})
            self.uidQueue.put(req)
            log.warning('self.uidQueue.size = ' + str(self.uidQueue.qsize()))

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

    def CreatedPageNum(self, num):
        for n in range(1, num+1):
            yield n

#   将各类请求随机取段，封装成request，加入requestQueue,
    def CreateInfoRequest(self, info, log):
        self.fillPage(info)
        fa_url = self.fans_url.format(self.uid)
        fo_url = self.fol_url.format(self.uid)
        mb_url = self.blog_url.format(self.uid)

        kind = [2, 3, 4]
        mblog = self.CreatedPageNum(self.pageOfmblog)
        fans = self.CreatedPageNum(self.pageOffans)
        follows = self.CreatedPageNum(self.pageOffol)
        intoErrorQueue = False
        while kind.__len__():

            size = self.requestQueue.qsize()
            log.warning('self.requestQueue.size = ' + str(size))
            errsize = self.errorQueue.qsize()
            log.warning('self.errorQueue.size = ' + str(errsize))

            if errsize > MaxErrorQueueSize and intoErrorQueue:
                for n in range(MaxErrorQueueSize):
                    temp = self.errorQueue.get()
                    self.requestQueue.put(temp)
                intoErrorQueue = False          

            if kind.__len__() != 1:
                choice = random.choice(kind)
                run = random.randint(3, 5)
            else:
                choice = kind[0]
                run = 99999

            if choice == 4:
                try:
                    for n in range(run):
                        page = next(mblog)
                        req = Request((mb_url % page),
                                        category = 4, meta = {'uid' : self.uid})
                        self.requestQueue.put(req)
                except StopIteration as st:
                    kind.remove(4)

            elif choice == 2:
                try:
                    for n in range(run):
                        page = next(fans)
                        req = Request((fa_url % page),
                                        category = 2, meta = {'uid' : self.uid})
                        self.requestQueue.put(req)                      
                except StopIteration as st:
                    kind.remove(2)

            else:
                try:
                    for n in range(run):
                        page = next(follows)
                        req = Request((fo_url % page),
                                        category = 3, meta = {'uid' : self.uid})
                        self.requestQueue.put(req)
                except StopIteration as st:
                    kind.remove(3)


        req = Request(self.detail_url.format(self.uid), category = 1, 
                                                meta = {'uid' : self.uid})
        print ('CreateInfoRequest ', self.uid)
        self.requestQueue.put(req)       
        intoErrorQueue = True

if __name__ == "__main__":
    dl = Schedule()
    dl.Start()
