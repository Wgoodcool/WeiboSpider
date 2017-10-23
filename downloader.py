# -*- coding: utf-8 -*-
import requests
from multiprocessing import Process
import random

from middle.transmission import Response
from middle.middlequeue import uidQueue, requestQueue
from middle.middlequeue import responseQueue, errorQueue, userResponseQueue
from middle.middlequeue import proxyQueue
from middle.settings import cookie#, useragent
from manager import InfoManager

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
        print ('userRequest')
        self.manager = InfoManager()
        self.manager.start()
        self.db_proxy = self.manager.proxyOpe()
        print (type(self.db_proxy))
        req = uqueue.get()
        print (type(req))

        while req:
            url = req.url
            cate = req.category
            meta = req.meta
            proxy = None
            header = self.GetHeader(cate)
            proxy = pro.get()

            try:
                req = requests.get(url, headers = header, proxies = proxy)
                print (type(req))
            except requests.exceptions.ConnectionError as pe:
                #self.db_proxy.delIp(proxy)
                uqueue.put(req)
                continue
            
            # 没有返回有用数据
            if (len(req.text) < 500) or req.status_code != 200:
                print ('Downloader Fail')
                uqueue.put(req)
                continue

            res = Response(url, cate, req.text, meta)
            print ('uidResponsePut')
            resqueue.put(res)

            req = uqueue.get()
        print ('end')
    def commonRequest (self, requeue, resqueue, equeue, pro):
        print ('commonRequest')
        self.manager = InfoManager()
        self.manager.start()
        self.db_proxy = self.manager.proxyOpe()
        req = requeue.get()

        while req:
            url = req.url
            cate = req.categoty
            meta = req.meta
            proxy = None
            header = self.GetHeader(cate)
# =============================================================================
#             测试暂时不需要代理
# =============================================================================
            proxy = pro.get()

            try:
                req = requests.get(url, headers = header, proxies = proxy)
            except requests.exceptions.ConnectionError as pe:
                equeue.put(req)
                req = requeue.get()
                self.db_proxy.delIp(proxy)
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
#        ug = random.choise(useragent)
        ug = "Mozilla/5.0 (Linux; U; Android 1.6; es-es; SonyEricssonX10i Build/R1FA016) AppleWebKit/528.5  (KHTML, like Gecko) Version/3.1.2 Mobile Safari/525.20.1"
        ck = random.choice(cookie)
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
        print ('Is Downloader')
        uid = Process(target = self.userRequest, args = (self.quid,
                                                         self.quser, self.qproxy))
        com = Process(target = self.commonRequest, args = (self.qresquest,
                                                           self.qresponse, self.qerror, self.qproxy))

        print ('Downloader Start')
        uid.start()
        com.start()
        uid.join()
        com.join()

if __name__ == "__main__":
    dl = Downloader()
    dl.Start()
