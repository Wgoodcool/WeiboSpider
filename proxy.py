# -*- coding: utf-8 -*-
from database import proxyOpe
from middle.middlequeue import proxyQueue
import time
import random

class ProxyCreate:
    def __init__(self):
        self.proxyQueue = proxyQueue

    def Start(self):
        print ('Is proxy')
        self.proxy = proxyOpe()
        while True:
            if (self.proxyQueue.qsize() > 10):
                time.sleep(random.randint(3, 6))
            det = None
            pro = self.proxy.getProxy()
            pro = ()
            if pro:
                res = '%s:%s' % pro
                det = {'https' : res}
            self.proxyQueue.put(det)
