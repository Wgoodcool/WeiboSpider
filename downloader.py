# -*- coding: utf-8 -*-
"""
Created on Mon Oct  9 20:56:11 2017

@author: wzx0518
"""
import requests
from multiprocessing import Process
import random

from database import proxyOpe
from middle.transmission import Response
from middle.middlequeue import uidQueue, requestQueue
from middle.middlequeue import responseQueue, errorQueue, userResponseQueue
from middle.middlequeue import proxyQueue
from middle.settings import useragent, cookie

class Downloader:
    def __init__(self):

#将队列拉入自己的类中
        self.quid = uidQueue
        self.qresquest = requestQueue
        self.qresponse = responseQueue  
        self.qerror = errorQueue
        self.quser = userResponseQueue
        self.qproxy = proxyQueue

    def userRequest (self, uqueue, resqueue, pro):
        proxy = proxyOpe()
        req = uqueue.get()

        while req:
            url = req.url
            cate = req.categoty()
            meta = req.meta()
            proxy = None
            header = self.GetHeader(cate)
            proxy = pro.get()

            try:
                req = requests.get(url, headers = header, proxies = proxy)
            except requests.exceptions.ConnectionError as pe:
                proxy.delIp(proxy)
                uqueue.put(req)
                continue

            # 没有返回有用数据
            if (len(req.text) < 500) or req.status_code != 200:
                uqueue.put(req)
                continue

            res = Response(url, cate, req.text, meta)
            resqueue.put(res)

            req = uqueue.get()

    def commonRequest (self, requeue, resqueue, equeue, pro):
        proxy = proxyOpe()
        req = requeue.get()

        while req:
            url = req.url
            cate = req.categoty()
            meta = req.meta()
            proxy = None
            header = self.GetHeader(cate)
            proxy = pro.get()

            try:
                req = requests.get(url, headers = header, proxies = proxy)
            except requests.exceptions.ConnectionError as pe:
                equeue.put(req)
                req = requeue.get()
                proxy.delIp(proxy)
                continue

            # 没有返回有用数据
            if (len(req.text) < 500) or req.status_code != 200:
                equeue.put(req)
                req = requeue.get()
                continue

            res = Response(url, cate, req.text, meta)
            resqueue.put(res)

            req = requeue.get()

    def GetHeader(self, cate):
        #header的发送顺序问题
        header = {}
        ug = random.choise(useragent)
        ck = random.choise(cookie)
        header = {
            'Accept' : 'application/json, text/plain, */*',
            'Accept-Encoding' : 'gzip, deflate, br',
            'Accept-Language' : 'zh-CN,zh;q=0.8',
            'Cache-Control' : 'no-cache',
            'Connection' : 'keep-alive',
            'User-Agent' : ug,
            'Cookie' : ck,
            'Host' : 'm.weibo.cn',
            'Upgrade-Insecure-Requests' : '1',
            'X-Requested-With' : 'XMLHttpRequest',
            'Pragma' : 'no-cache',
        }
        if cate != 0:
            header['Referer'] = ''
        return header

    def Start(self):
        uid = Process(target = self.userRequest, args = (self.quid,
                                                         self.quser, self.qproxy))
        com = Process(target = self.commonRequest, args = (self.qresquest,
                                                           self.qresponse, self.qerror, self.qproxy))
        uid.start()
        com.start()
        uid.join()
        com.join()

if __name__ == "__main__":
    dl = Downloader()
    dl.Start()
