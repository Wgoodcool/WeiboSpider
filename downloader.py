from middle.transmission import Response
from middle.middlequeue import uidQueue, requestQueue
from middle.middlequeue import responseQueue, errorQueue, userResponseQueue
from middle.middlequeue import proxyQueue
from middle.middlequeue import logQueue
from middle.settings import cookie#, UserAgent
from middle.settings import ProcessNumber, uidGeventProcessNumber, infoGeventProcessNumber
from middle.settings import maxuidCoroutineNum, maxinfoCoroutineNum
from manager import InfoManager

import logging
from logging.handlers import QueueHandler
import requests
from multiprocessing import Process, Queue
import random
import gevent.pool  
import gevent.monkey
from gevent.lock import Semaphore
#除了网络阻塞取消，其他阻塞均保持原样
gevent.monkey.patch_socket()

class Downloader:
    def __init__(self):
#将队列拉入自己的类中
        self.quid = uidQueue
        self.qresquest = requestQueue
        self.qresponse = responseQueue  
        self.qerror = errorQueue
        self.quser = userResponseQueue
        self.qproxy = proxyQueue
        #日志队列
        self.logqueue = logQueue

        #通过队列的阻塞来控制启动的最大进程数
        self.ProcessNumberQueue = Queue(ProcessNumber)    

        #控制gevent一次处理的Request数  
        self.uidGeventProcessNumber = uidGeventProcessNumber
        self.infoGeventProcessNumber = infoGeventProcessNumber

    def getLog(self, name):
        print ('getLog')
        logger = logging.getLogger(name)
        queue_handler = QueueHandler(self.logqueue)
        logger.addHandler(queue_handler)

        return logger

    def Start(self):
        print ('Is Downloader')
        log = self.getLog('Downloader')
        log.warning('Start')
        uid = Process(target = self.userRequestManager)
        com = Process(target = self.infoRequestManager)

        uid.start()
        com.start()
        uid.join()
        com.join()

        log.warning('End!')

    def userRequestManager(self):
        print ('userRequestManager')
        log = self.getLog('userRequestManager')
        log.warning('Start')
        req = self.quid.get()
        requestList = []
        while req:
            requestList.append(req)
            req = self.quid.get()
            if len(requestList) == self.uidGeventProcessNumber:
                p = Process(target = self.userGevent,
                                args = (requestList, ))
                p.start()
                requestList = []
                #达到一定大小会阻塞，除非userRequest完成所有的抓取工作
                self.ProcessNumberQueue.put(0)
                log.warning('Create Process')

    def userGevent(self, requestList):
        print ('userGevent')
        log = self.getLog('Downloader.userGevent')
        log.warning('Start')
        self.manager = InfoManager()
        self.manager.start()
        self.db_proxy = self.manager.proxyOpe()
        self.sem = Semaphore(maxuidCoroutineNum)

        task = []
        for req in requestList:
            task.append(gevent.spawn(self.userFetch, req))
        gevent.joinall(task)
        log.warning('All End')
        self.ProcessNumberQueue.get()

    def userFetch(self, req):
        url = req.url
        cate = req.category
        meta = req.meta
        header = self.GetHeader(cate)
#        proxy = self.qproxy.get()
        proxy = None

        try:
            self.sem.acquire()
            req = requests.get(url, headers = header, proxies = proxy)
            print (type(req))
        except requests.exceptions.ConnectionError as pe:
            #self.db_proxy.delIp(proxy)
            self.quid.put(req)
        finally:
            self.sem.release()

        # 没有返回有用数据
        if (len(req.text) < 500) or req.status_code != 200:
            print ('Downloader Fail')
            self.quid.put(req)

        res = Response(url, cate, req.text, meta)
        self.quser.put(res)

    def infoRequestManager(self):
        print ('infoRequestManager')
        log = self.getLog('Downloader.infoRequestManager')
        log.warning('Start')
        infoReqList = []
        req = self.qresquest.get()
        while req:
            infoReqList.append(req)
            req = self.qresquest.get()
            if len(infoReqList) == self.infoGeventProcessNumber:
                p = Process(target = self.infoGevent,
                                args = (infoReqList, ))
                p.start()
                infoReqList = []
                self.ProcessNumberQueue.put(0)

    def infoGevent(self, infoReqList):
        log = self.getLog('Downloader.infoGevent')
        log.warning('Start')
        #一次允许并发的最大协程数
        self.sem = Semaphore(maxinfoCoroutineNum)
        self.manager = InfoManager()
        self.manager.start()
        self.db_proxy = self.manager.proxyOpe()

        task = []
        for req in infoReqList:
            task.append(gevent.spawn(self.infoFetch, req))
        gevent.joinall(task)

        log.warning('Process End')
        self.ProcessNumberQueue.get()

    def infoFetch(self, req):
        url = req.url
        cate = req.categoty
        meta = req.meta
        header = self.GetHeader(cate)
# =============================================================================
#             测试暂时不需要代理
# =============================================================================
        # proxy = self.qproxy.get()
        proxy = None

        try:
            self.sem.acquire()
            req = requests.get(url, headers = header, proxies = proxy)
        except requests.exceptions.ConnectionError as pe:
            self.qerror.put(req)
            # self.db_proxy.delIp(proxy)
        finally:
            self.sem.release()

        # 没有返回有用数据
        #代理池增加一个字段，此处失败一次，该字段自增1.大于一定值删除
        if (len(req.text) < 500) or req.status_code != 200:
            self.qerror.put(req)
            self.log.warning('Fail')
        else:
            res = Response(url, cate, req.text, meta)
            self.qresponse.put(res)

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

if __name__ == "__main__":
    dl = Downloader()
    dl.Start()
