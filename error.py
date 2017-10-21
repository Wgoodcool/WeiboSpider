# -*- coding: utf-8 -*-
import requests
import random

from middle.middlequeue import responseQueue, errorQueue, proxyQueue
from middle.settings import useragent, cookie
from database import proxyOpe
from middle.transmission import Response

class ErrorDeal:
    
    def Start(self):
        db_proxy = proxyOpe()
        req = errorQueue.get()
        while req:
            url = req.url
            cate = req.categoty()
            meta = req.meta()
            proxy = None
            header = self.GetHeader(cate)
            proxy = proxyQueue.get()
            try:
                req = requests.get(url, headers = header, proxies = proxy)
            except requests.exceptions.ConnectionError as pe:
                errorQueue.put(req)
                req = errorQueue.get()
                db_proxy.delIp(proxy)
                continue

            # 没有返回有用数据
            if (len(req.text) < 500) or req.status_code != 200:
                errorQueue.put(req)
                req = errorQueue.get()
                continue

            res = Response(url, cate, req.text, meta)
            responseQueue.put(res)            
            
            req = errorQueue.get()

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
            'Pragma' : 'no-cache'
        }
        if cate != 0:
            header['Referer'] = ''
        return header    

if __name__ == "__main__":
    dl = ErrorDeal()
    dl.Start()
