# -*- coding: utf-8 -*-
# =============================================================================
# TODO: 数据库操作类需要考虑并发问题,数据库操作类需要加锁
# =============================================================================
from middle.middlequeue import responseQueue, userResponseQueue
from middle.middlequeue import logQueue
from manager import InfoManager

import logging
from logging.handlers import QueueHandler
from multiprocessing import Process
import json
import re
import datetime

class WeiboSpider:
    def __init__(self):
        self.responseQueue = responseQueue
        self.userResponseQueue = userResponseQueue
        self.logqueue = logQueue

    def getLog(self, name):
        logger = logging.getLogger(name)
        queue_handler = QueueHandler(self.logqueue)
        logger.addHandler(queue_handler)

        return logger

    def Start(self):
        print ('Is Spider')
        log = self.getLog('Spider')
        com = Process(target = self.CommonSpider, args = (self.responseQueue, ))
        user = Process(target = self.UserSpider, args = (self.userResponseQueue, ))
        log.warning('Start')
        print ('Spider Start')
        com.start()
        user.start()

        com.join()
        user.join()

    def CommonSpider(self, queue):
        log = self.getLog('Spider.CommonSpider')
        log.warning('Start')

        self.manager = InfoManager()
        self.manager.start()
# 分别用于转换表情，去除HTML标签，提取数字，去除他人转发评论
        self.emotion = re.compile('<span.*?url-icon.*?alt="(.*?)">.*?</span>')
        self.tag = re.compile(r'<[^>]+>',re.S)
        self.number = re.compile('\D')
        self.raw_test = '//@'
#用于匹配日期
        self.minutes = '分钟'
        self.hour = '小时'
        self.yeaterday = '昨天'
#    数据库操作类

        self.db_fan = self.manager.FanOpe()
        self.db_fol = self.manager.FolOpe()
        self.db_mb = self.manager.MblogOpe()

        print ('CommonSpider')
        res = queue.get()
        print ('CommonSpider Get')
        log.warning('CommonSpider Get')

        while res:
            if res.category == 1:
                print ('1')
                result = self.getDetail(res.text)
                self.db_user.update(result)

            elif res.category == 2:
                print ('2')
                result = self.getFan(res.text)
                self.db_fan.insert(res.meta['uid'], result)

            elif res.category == 3:
                print ('3')
                result = self.getFol(res.text)
                self.db_fol.insert(res.meta['uid'], result)

            else:
                print ('4')
                result = self.getMblog(res.text)
                temp = self.getMblog(result)
                self.db_mb.insert(temp)
            res = queue.get()

    def UserSpider(self, queue):
        self.manager = InfoManager()
        self.manager.start()
        self.db_user = self.manager.UserOpe()
        log = self.getLog('Spider.UserSpider')
        log.warning('Start')

        print ('UserSpider')
        res = queue.get()
        print ('UserSpider Get')
        log.warning('UserSpider Get')
        while res:
            if res.category == 0:
                result = self.getUserInfo(res.text)
                self.db_user.insert(result)
            res = queue.get()

#从字典中获取相应的值
    def getUserInfo(self, res):
        js = json.loads(res)
        user = js['userInfo']
        temp = []
        temp.append(user['id'])
        temp.append(user['statuses_count'])
        temp.append(user['followers_count'])
        temp.append(user['follow_count'])
        temp.append(user.get('urank', -1))
        temp.append(user['screen_name'])
        temp.append(user['description'])
        temp.append(user.get('gender', 'NULL'))
        temp.append(user.get('verified_reason', 'NULL'))
        return temp

    def getDetail(self, res):
        js = json.loads(res)
        info = js['cards']
        fir = info[0]['card_group']
        sec = info[1]['card_group']
        temp = []
        #地址
        temp.append(fir[4]['item_content'])
        #注册时间
        temp.append(sec[2]['item_content'])
        #信用
        temp.append(sec[1]['item_content'])
        #标签
        temp.append(fir[2]['item_content'])
        del js
        return temp

    def getFan(self, res):
        js = json.loads(res)
        js = js['cards'][0]['card_group']
        temp = []
        for num in range(len(js)):
            temp.append(js[num]['user']['id'])
        del js
        return temp

    def getFol(self, res):
        js = json.loads(res)
        js = js['cards'][0]['card_group']
        temp = []
        for num in range(len(js)):
            temp.append(js[num]['user']['id'])
        del js
        return temp

#用于获取微博发表时间
    def getDate(self, date):
        if date.find(self.minutes) != -1:
            return datetime.datetime.now().strftime('%Y-%m-%d')
        elif date.find(self.hour) != -1:
            num = int(self.number.sub('', date))
            date = datetime.datetime.now() - datetime.timedelta(hours=num)
            return date.strftime('%Y-%m-%d')
        elif date.find(self.yeaterday) != -1:
            now = datetime.datetime.now()
            yes = datetime.timedelta(days=1)
            return (now - yes).strftime('%Y-%m-%d')
        else:
            return date

    def clean(self, text):
        emoList = self.emotion.findall(text)
        for e in emoList:
            text = self.emotion.sub(e, text, count = 1)
        text = self.tag.sub('', text)

        return text

    def getMblog(self, res):
        js = json.loads(res)
        js = js['cards']
        temp = []
        for num in range(len(js)):
            det = js[num]
            if 'card_group' in det.keys():
                continue
            det = det['mblog']

            arr = {}
            arr['id'] = det['user']['id']
            arr['mid'] = det['mid']
            arr['attitudes_count'] = det['attitudes_count']
            arr['reposts_count'] = det['reposts_count']
            arr['comments_count'] = det['comments_count']
            arr['source'] = det['source']

            if 'retweeted_status' in det.keys():
                arr['retweeted_status'] = det['retweeted_status']['mid']
                text = det['raw_text']
                text = text[ : text.find(self.raw_test)]
                text = self.tag.sub('', text)
                arr['text'] = text
            else:
                arr['retweeted_status'] = -1
                arr['text'] = self.clean(det['text'])

            if 'pics' in det.keys():
                arr['pic_number'] = len(det['pics'])
            else:
                arr['pic_number'] = 0

            arr['textLength'] = len(arr['text'])
            arr['create_at'] = self.getDate(det['created_at'])

            temp.append(arr)
        del js
        temp = self.dic2list(temp)
        return temp

    def dic2list(self, temp):
        res = []
        for blog in range(len(temp)):
            arr = []
            arr.append(blog['id'])
            arr.append(blog['mid'])
            arr.append(blog['attitudes_count'])
            arr.append(blog['reposts_count'])
            arr.append(blog['comments_count'])
            arr.append(blog['retweeted_status'])
            arr.append(blog['pic_number'])
            arr.append(blog['textLength'])
            arr.append(blog['source'])
            arr.append(blog['text'])
            arr.append(blog['create_at'])
            res.append(arr)
        return res

if __name__ == "__main__":
    dl = WeiboSpider()
    dl.Start()
