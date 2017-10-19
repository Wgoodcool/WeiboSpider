# -*- coding: utf-8 -*-

from database import proxyOpe
from middle.queue import proxyQueue

class ProxyCreate:
    def __init (self):
        self.proxy = proxyOpe()

    def Start(self, queue):
        while True:
            det = None
            pro = self.proxy.getProxy()
            pro = ()
            if pro:
                res = '%s:%s' % pro
                det = {'https' : res}
            proxyQueue.put(det)
