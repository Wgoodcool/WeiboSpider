from middle.transmission import Response
from middle.middlequeue import uidQueue, requestQueue
from middle.middlequeue import responseQueue, errorQueue
from middle.middlequeue import proxyQueue
from middle.middlequeue import logQueue
from middle.settings import cookie, UserAgent
from middle.settings import ProcessNumber, uidGeventProcessNumber, infoGeventProcessNumber
from middle.settings import maxuidCoroutineNum, maxinfoCoroutineNum
from middle.settings import MaxrequestQueueSize, MaxRequestTime
from manager import InfoManager

import time
import logging
from logging.handlers import QueueHandler
import requests
from multiprocessing import Process, Queue
import random
import gevent
import gevent.monkey
from gevent.lock import Semaphore
#除了网络阻塞取消，其他阻塞均保持原样
gevent.monkey.patch_socket()
import traceback
import hashlib

class Downloader:
    def __init__(self):
#将队列拉入自己的类中
        self.quid = uidQueue
        self.qresquest = requestQueue
        self.qresponse = responseQueue  
        self.qerror = errorQueue
        # self.qproxy = proxyQueue
        #日志队列
        self.logqueue = logQueue
        self.log = self.getLog('Downloader')

        #通过队列的阻塞来控制启动的最大进程数
        self.userProcessNumberQueue = Queue(ProcessNumber)    
        self.infoProcessNumberQueue = Queue(ProcessNumber)
        #控制gevent一次处理的Request数  
        self.uidGeventProcessNumber = uidGeventProcessNumber
        self.infoGeventProcessNumber = infoGeventProcessNumber
        #代理ip地址
        self.proxy = {'https' : 'https://forward.xdaili.cn:80'}

    def getLog(self, name):
        logger = logging.getLogger(name)
        queue_handler = QueueHandler(self.logqueue)
        logger.addHandler(queue_handler)

        return logger

    def Start(self):
        self.log.warning('Start')
        try:
            uid = Process(target = self.userRequestManager)
            com = Process(target = self.infoRequestManager)

            uid.start()
            com.start()
            uid.join()
            com.join()
        except KeyboardInterrupt as ki:
            self.log.warning('self.quid.qsize() = ' + self.quid.qsize())
            self.log.warning('self.qresquest.qsize() = ' + self.qresquest.qsize())
            self.log.warning('self.qresponse.qsize() = ' + self.qresponse.qsize()) 
            self.log.warning('self.qerror.qsize() = ' + self.qerror.qsize())
            self.log.warning('self.logqueue.qsize() = ' + self.logqueue.qsize())
        except Exception as e:
            with open('Downloader_Start.txt', 'a+') as f:
                traceback.print_exc(file=f)
                f.write(repr(e))
                f.write('\n')

        self.log.warning('End!')

    def userRequestManager(self):
        self.log.warning('userRequestManager Start')
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
                self.log.warning('self.userProcessNumberQueue.size = ' + str(self.userProcessNumberQueue.qsize()))
                self.userProcessNumberQueue.put(0)
                self.log.warning('Create Process')

    def userGevent(self, requestList):
        self.log.warning('userGevent Start')
        self.sem = Semaphore(maxuidCoroutineNum)

        start = time.clock()
        task = []
        try:
            for req in requestList:
                task.append(gevent.spawn(self.userFetch, (req, self.log)))
            gevent.joinall(task)
        except Exception as e:
            with open('userGevent.txt', 'a+') as f:
                traceback.print_exc(file=f)
                f.write(repr(e))
                f.write('\n')
        end = time.clock()

        self.log.warning('All End, Use Time is ' + str(end-start))
        self.userProcessNumberQueue.get()

    def userFetch(self, param):
        req = param[0]
        url = req.url
        cate = req.category
        meta = req.meta
        header = self.GetHeader(cate)
        # proxy = self.qproxy.get()

        log = param[1]

        if req.time > MaxRequestTime:
            return

        userRes = None
        try:
            self.sem.acquire()
            # userRes = requests.get(url, headers = header, proxies = proxy, timeout = 5)
            userRes = requests.get(url, headers = header, proxies = self.proxy,
                                         timeout = 5, verify=False,allow_redirects=False)
        except requests.exceptions.ConnectionError as pe:
            log.warning('Connection')
            self.quid.put(req)
        except requests.exceptions.Timeout as time:
            log.warning('Timeout')
            self.quid.put(req)
        except requests.RequestException as exc:
            log.warning('RequestException')
            self.quid.put(req)
        finally:
            self.sem.release()
            log.warning('self.quid.size = ' + str(self.quid.qsize()))

        if userRes is None:
            return

        # 没有返回有用数据
        if (len(userRes.text) < 500) or (userRes.status_code != 200):
            req.time += 1
            self.quid.put(req)
            log.warning('Unuse Data')
            log.warning(userRes.text)
        else:
            log.warning('Success Download')
            userResponse = Response(url, cate, userRes.text, meta)
            self.qresponse.put(userResponse)

    def infoRequestManager(self):
        self.log.warning('infoRequestManager Start')
        infoReqList = []
        req = self.qresquest.get()
        try:
            while req:
                infoReqList.append(req)
                req = self.qresquest.get()
                if len(infoReqList) == self.infoGeventProcessNumber:
                    p = Process(target = self.infoGevent,
                                    args = (infoReqList, ))
                    p.start()
                    infoReqList = []
                    self.log.warning('self.infoProcessNumberQueue.size = ' + str(self.infoProcessNumberQueue.qsize()))
                    self.infoProcessNumberQueue.put(0)
                    self.log.warning('Create Process')
        except KeyboardInterrupt as KI:
            self.log.warning('self.qresquest.qsize() = ' + self.qresquest.qsize())
        except Exception as e:
            with open('infoRequestManager.txt', 'a+') as f:
                traceback.print_exc(file = f)
                f.write(repr(e))
                f.write('\n')

    def infoGevent(self, infoReqList):
        self.log.warning('infoGevent Start')
        #一次允许并发的最大协程数
        self.sem = Semaphore(maxinfoCoroutineNum)
        self.manager = InfoManager()
        self.manager.start()
        try:
            start = time.clock()
            task = []
            for req in infoReqList:
                task.append(gevent.spawn(self.infoFetch, (req, self.log)))
            gevent.joinall(task)
            end = time.clock()
        except Exception as e:
            with open('infoGevent.txt', 'a+') as f:
                traceback.print_exc(file=f)
                f.write(repr(e))
                f.write('\n')
        self.log.warning('Process End, Use Time Is ' + str(end - start))
        self.infoProcessNumberQueue.get()

    def infoFetch(self, param):
        log = param[1]
        req = param[0]
        url = req.url
        cate = req.category
        meta = req.meta
        header = self.GetHeader(cate)
        # proxy = self.qproxy.get()

        if req.time > MaxRequestTime:
            return

        infoRes = None
        try:
            self.sem.acquire()
            infoRes = requests.get(url, headers = header, proxies = self.proxy,
                                timeout = 5, verify=False, allow_redirects=False)
            # infoRes = requests.get(url, headers = header, proxies = proxy, timeout = 5)
        except requests.exceptions.ConnectionError as pe:
            log.warning('ConnectionError')
            self.qerror.put(req)
        except requests.exceptions.Timeout as time:
            log.warning('TimeoutError')
            self.qerror.put(req)
        except requests.RequestException as exc:
            error = repr(exc)
            log.warning('RequestException ' + error)
            self.qerror.put(req)
        finally:
            self.sem.release()
            log.warning('self.qresponse.size = ' + str(self.qresponse.qsize()))
            log.warning('self.qresquest.size = ' + str(self.qresquest.qsize()))

        if infoRes is None:
            return

        # 没有返回有用数据
        if (len(infoRes.text) < 800) or (infoRes.status_code != 200):
            req.time += 1    
            self.qerror.put(req)
            log.warning(url + ' Fail')
        else:
            infoResponse = Response(url, cate, infoRes.text, meta)
            log.warning('Succeed')
            self.qresponse.put(infoResponse)

    def getIp(self):
        orderno = 'ZF20171158496HBnRg3'
        secret = '75c5f022a9e34ad1a8d95190b32a170d'
        timestamp = str(int(time.time()))
        string = 'orderno={},secret={},timestamp={}'.format(orderno, secret, timestamp)
        string = string.encode()
        md5 = hashlib.md5(string).hexdigest()
        sign = md5.upper()
        auto = 'sign={}&orderno={}&timestamp={}'.format(sign, orderno, timestamp)

        return auto

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
            'Proxy-Authorization' : self.getIp()
        }
        # if cate != 0:
        #     header['Referer'] = ''
        return header

if __name__ == "__main__":
    dl = Downloader()
    dl.Start()
