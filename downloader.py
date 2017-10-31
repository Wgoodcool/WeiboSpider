from middle.transmission import Response
from middle.middlequeue import uidQueue, requestQueue
from middle.middlequeue import responseQueue, errorQueue, userResponseQueue
from middle.middlequeue import proxyQueue
from middle.settings import cookie#, UserAgent
from middle.settings import ProcessNumber, uidGeventProcessNumber, infoGeventProcessNumber
from middle.settings import maxuidCoroutineNum, maxinfoCoroutineNum
from manager import InfoManager

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
        #方便传参
        self.uidQueueList = [self.quid, self.quser, self.qproxy]
        self.resQueueList = [self.qresquest, self.qresponse, self.qerror, self.qproxy]

        #通过队列的阻塞来控制启动的最大进程数
        self.ProcessNumberQueue = Queue(ProcessNumber)    
        #控制gevent一次处理的Request数  
        self.uidGeventProcessNumber = uidGeventProcessNumber
        self.infoGeventProcessNumber = infoGeventProcessNumber

        self.uidNumberQueue = [self.ProcessNumberQueue, self.uidGeventProcessNumber]
        self.infoNumberQueue = [self.ProcessNumberQueue, self.infoGeventProcessNumber]
            
    def Start(self):
        print ('Is Downloader')

        uid = Process(target = self.userRequestManager, args = (self.uidQueueList, self.uidNumberQueue))
        com = Process(target = self.infoRequestManager, args = (self.resQueueList, self.infoNumberQueue))

        print ('Downloader Start')
        uid.start()
        com.start()
        uid.join()
        com.join()

    def userRequestManager(self, QueueList, maxValue):
        print ('userRequestManager')
        self.uidReqQueue = QueueList[0]
        req = self.uidReqQueue.get()
        requestList = []
        while req:
            requestList.append(req)
            req = self.uidReqQueue.get()
            if len(requestList) == maxValue[1]:
                p = Process(target = self.userGevent,
                                args = (requestList, QueueList, maxValue[0]))
                p.start()
                requestList = []
            #达到一定大小会阻塞，除非userRequest完成所有的抓取工作
            maxValue[0].get()

    def userGevent(self, requestList, QueueList, maxNumQueue):
        print ('userGevent')
        self.manager = InfoManager()
        self.manager.start()
        self.sem = Semaphore(maxuidCoroutineNum)
        self.db_proxy = self.manager.proxyOpe()
        self.uqueue = QueueList[0]
        self.resqueue = QueueList[1]
        self.proxy = QueueList[2]

        task = []
        for req in requestList:
            task.append(gevent.spawn(self.userFetch, req))
        gevent.joinall(task)
        maxNumQueue.put(0)

    def userFetch(self, req):
        print ('userFetch')
        url = req.url
        cate = req.category
        meta = req.meta
        header = self.GetHeader(cate)
#        proxy = self.proxy.get()
        proxy = None

        try:
            self.sem.acquire()
            req = requests.get(url, headers = header, proxies = proxy)
            print (type(req))
        except requests.exceptions.ConnectionError as pe:
            #self.db_proxy.delIp(proxy)
            self.uqueue.put(req)
        finally:
            self.sem.release()

        # 没有返回有用数据
        if (len(req.text) < 500) or req.status_code != 200:
            print ('Downloader Fail')
            self.uqueue.put(req)

        res = Response(url, cate, req.text, meta)
        self.resqueue.put(res)

        req = self.uqueue.get()

    def infoRequestManager(self, QueueList, maxValue):
        print ('infoRequestManager')
        self.infoQueue = QueueList[0]
        infoReqList = []

        req = self.infoQueue.get()
        while req:
            infoReqList.append(req)
            req = self.infoQueue.get()
            if len(infoReqList) == maxValue[1]:
                p = Process(target = self.infoGevent,
                                args = (infoReqList, QueueList, maxValue[0]))
                p.start()
                infoReqList = []
            maxValue[0].get()

    def infoGevent(self, infoReqList, QueueList, maxNum):
        #一次允许并发的最大协程数
        self.sem = Semaphore(maxinfoCoroutineNum)
        print ("infoGevent")
        self.manager = InfoManager()
        self.manager.start()
        self.db_proxy = self.manager.proxyOpe()
        self.requeue = QueueList[0]
        self.resqueue = QueueList[1]
        self.equeue = QueueList[2]
        self.pro = QueueList[3]

        task = []
        for req in infoReqList:
            task.append(gevent.spawn(self.infoFetch, req))
        gevent.joinall(task)
        maxNum.put(0)

    def infoFetch(self, req):
        print ('infoFetch')
        url = req.url
        cate = req.categoty
        meta = req.meta
        header = self.GetHeader(cate)
# =============================================================================
#             测试暂时不需要代理
# =============================================================================
        # proxy = self.pro.get()
        proxy = None

        try:
            self.sem.acquire()
            req = requests.get(url, headers = header, proxies = proxy)
        except requests.exceptions.ConnectionError as pe:
            self.equeue.put(req)
            self.db_proxy.delIp(proxy)
        finally:
            self.sem.release()

        # 没有返回有用数据
        #代理池增加一个字段，此处失败一次，该字段自增1.大于一定值删除
        if (len(req.text) < 500) or req.status_code != 200:
            self.equeue.put(req)
        else:
            res = Response(url, cate, req.text, meta)
            self.resqueue.put(res)

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
