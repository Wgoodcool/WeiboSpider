# -*- coding: utf-8 -*-
from multiprocessing import Queue

#Schedule给Downloader传输用
requestQueue = Queue(50)
#downloader给spider传输普通response用
responseQueue = Queue()
#schedule给downloader传输用户ID请求用
uidQueue = Queue(50)
#downloader给spider传输用户初次信息用
userResponseQueue = Queue()
#下载错误队列
errorQueue = Queue()
#proxy通道
proxyQueue = Queue(100)
#日志队列
logQueue = Queue()
