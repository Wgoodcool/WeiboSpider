#==================================================================================
# 更新说明
# 买了一堆代理ip，启用了代理进行抓取，但是该代理的有效率低下，相应时间较慢
# 解决了协程处理时的问题，前一份代码在userFetch()和infoFetch()会出现没有赋值就引用的错误
# 其他零零碎碎完善
#==================================================================================
from middle.transmission import Response
from middle.middlequeue import uidQueue, requestQueue
from middle.middlequeue import responseQueue, errorQueue, userResponseQueue
from middle.middlequeue import proxyQueue
from middle.middlequeue import logQueue
from middle.settings import cookie, UserAgent
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
        logger = logging.getLogger(name)
        queue_handler = QueueHandler(self.logqueue)
        logger.addHandler(queue_handler)
        logger.warning('GetLog')

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
        proxy = self.qproxy.get()
        # proxy = None

        try:
            self.sem.acquire()
            userRes = requests.get(url, headers = header, proxies = proxy, timeout = 5)
        except requests.exceptions.ConnectionError as pe:
            self.quid.put(req)
        except requests.exceptions.Timeout as time:
            self.quid.put(req)
        finally:
            self.sem.release()
            return
        # 没有返回有用数据
        if (len(userRes.text) < 500) or (userRes.status_code != 200):
            print ('Downloader Fail')
            self.quid.put(req)

        userResponse = Response(url, cate, userRes.text, meta)
        self.quser.put(userResponse)

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
        print ('infoGevent')
        log = self.getLog('Downloader.infoGevent')
        log.warning('Start')
        #一次允许并发的最大协程数
        self.sem = Semaphore(maxinfoCoroutineNum)
        self.manager = InfoManager()
        self.manager.start()
        self.db_proxy = self.manager.proxyOpe()

        task = []
        for req in infoReqList:
            task.append(gevent.spawn(self.infoFetch, (req, log)))
        gevent.joinall(task)

        log.warning('Process End')
        self.ProcessNumberQueue.get()

    def infoFetch(self, param):
        log = param[1]
        req = param[0]
        url = req.url
        cate = req.category
        meta = req.meta
        header = self.GetHeader(cate)
        proxy = self.qproxy.get()

        try:
            self.sem.acquire()
            infoRes = requests.get(url, headers = header, proxies = proxy, timeout = 5)
        except requests.exceptions.ConnectionError as pe:
            self.qerror.put(req)
        except requests.exceptions.Timeout as time:
            self.qerror.put(req)
        finally:
            self.sem.release()
            return

        # 没有返回有用数据
        if (len(infoRes.text) < 500) or (infoRes.status_code != 200):
            self.qerror.put(req)
            log.warning('Fail')
        else:
            infoResponse = Response(url, cate, infoRes.text, meta)
            log.warning('Succeed')
            self.qresponse.put(infoResponse)

    def GetHeader(self, cate):
        #header的发送顺序问题
        header = {}
        ug = random.choice(UserAgent)
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
