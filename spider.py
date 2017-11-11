# -*- coding: utf-8 -*-
# =============================================================================
# TODO: 数据库操作类需要考虑并发问题,数据库操作类需要加锁
# =============================================================================
from middle.middlequeue import responseQueue, userResponseQueue
from middle.middlequeue import logQueue
#from manager import InfoManager
from database import UserOpe, FanOpe, FolOpe, MblogOpe

import logging
from logging.handlers import QueueHandler
from multiprocessing import Process
import json
import re
import datetime
import traceback
import pymysql as pm

class WeiboSpider:
    def __init__(self):
        self.responseQueue = responseQueue
        self.userResponseQueue = userResponseQueue
        self.logqueue = logQueue
        self.log = self.getLog('Spider')

    def getLog(self, name):
        logger = logging.getLogger(name)
        queue_handler = QueueHandler(self.logqueue)
        logger.addHandler(queue_handler)

        return logger

    def Start(self):
        try:
            com = Process(target = self.CommonSpider, args = (self.responseQueue, ))
            user = Process(target = self.UserSpider, args = (self.userResponseQueue, ))
            self.log.warning('Start')
            com.start()
            user.start()

            com.join()
            user.join()
        except KeyboardInterrupt as Ki:
            self.log.warning('self.responseQueue.qize() = ' + self.responseQueue.qsize())
            self.log.warning('self.userResponseQueue.qsize() = ' + self.userResponseQueue.qsize())
        except Exception as e:
            with open('Spider_Start.txt', 'a+') as f:
                traceback.print_exc(file=f)
                f.write(repr(e))
                f.write('\n')
    def CommonSpider(self, queue):
        self.log.warning('CommonSpider Start')

        # self.manager = InfoManager()
        # self.manager.start()
        # self.db_fan = self.manager.FanOpe()
        # self.db_fol = self.manager.FolOpe()
        # self.db_mb = self.manager.MblogOpe()
        self.db_fan = FanOpe()
        self.db_fol = FolOpe()
        self.db_mb = MblogOpe()
        db_user = UserOpe()
# 分别用于转换表情，去除HTML标签，提取数字，去除他人转发评论
        self.emotion = re.compile('<span.*?url-icon.*?alt="(.*?)">.*?</span>')
        self.tag = re.compile(r'<[^>]+>',re.S)
        self.number = re.compile('\D')
        self.raw_test = '//@'
#用于匹配日期
        self.minutes = '分钟'
        self.hour = '小时'
        self.yeaterday = '昨天'
        self.datesign = '-'
        self.year = str(datetime.datetime.now().year)
#    数据库操作类

        res = queue.get()
        self.log.warning('CommonSpider Get')

        while res:
            self.log.warning('queue.size = ' + str(queue.qsize()))
            if res.category == 1:
                try:
                    result = self.getDetail(res.text)
                except Exception as e:
                    with open('spider_info.txt', 'a+') as f:
                        f.write(res.url)
                        f.write('\n')
                        f.write(res.text)
                        f.write('\n')
                        traceback.print_exc(file = f)
                        f.write(repr(e))
                        f.write('\n\n')
                else:
                    temp = res.meta['uid']
                    db_user.update(temp, result, res.url)
                    self.log.warning('Update ' + str(temp))

            elif res.category == 2:
                try:
                    result = self.getFan(res.text)
                except Exception as e:
                    with open('spider_fan.txt', 'a+') as f:
                        f.write(res.url)
                        f.write('\n')
                        f.write(res.text)
                        f.write('\n')
                        traceback.print_exc(file = f)
                        f.write(repr(e))
                    #测试使用return，直接结束,正式运行采用continue
                    self.log.warning('The Spider Is Stop')
                    return
                else:
                    self.db_fan.insert(res.meta['uid'], result, res.url)
                    self.log.warning('Fan Insert ' + str(res.meta['uid']))

            elif res.category == 3:
                try:
                    result = self.getFol(res.text)
                except Exception as e:
                    with open('spider_follow.txt', 'a+') as f:
                        f.write(res.url)
                        f.write('\n')
                        f.write(res.text)
                        f.write('\n')
                        traceback.print_exc(file = f)
                        f.write('\n')
                        f.write(repr(e))
                    #测试使用return，直接结束,正式运行采用continue
                    self.log.warning('The Spider Is Stop')
                    return
                else:
                    self.db_fol.insert(res.meta['uid'], result, res.url)
                    self.log.warning('Follow Insert ' + str(res.meta['uid']))

            else:
                try:
                    result = self.getMblog(res.text)
                except Exception as e:
                    with open('spider_mblog.txt', 'a+') as f:
                        f.write(res.url)
                        f.write('\n')
                        f.write(res.text)
                        f.write('\n')
                        traceback.print_exc(file = f)
                        f.write(repr(e))
                    #测试使用return，直接结束,正式运行采用continue
                    self.log.warning('The Spider Is Stop')
                    return
                else:
                    self.db_mb.insert(result, res.url)
                    self.log.warning('Mblog Insert ' + str(res.meta['uid']))
            res = queue.get()

    def UserSpider(self, queue):
        # self.manager = InfoManager()
        # self.manager.start()
        # self.db_user = self.manager.UserOpe()
        db_user = UserOpe()
        self.log.warning('UserSpider Start')

        res = queue.get()
        while res:
            self.log.warning('queue.size = ' + str(queue.qsize()))
            if res.category == 0:
                try:
                    result = self.getUserInfo(res.text)
                except Exception as e:
                    with open('UserSpider.txt', 'a+') as f:
                        f.write(res.url)
                        f.write('\n')
                        f.write(res.text)
                        f.write('\n')
                        traceback.print_exc(file = f)
                        f.write(repr(e))
                else:
                    db_user.insert(result)
                    self.log.warning('User Insert ' + str(result[0]))
            res = queue.get()
            self.log.warning('Get')

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

        name = pm.escape_string(user.get('screen_name', ''))
        temp.append(name)

        des = user.get('description', '')
        des = pm.escape_string(des)
        temp.append(des)

        temp.append(user.get('gender', ''))
        temp.append(user.get('verified_reason', ''))
        return temp

    def getDetail(self, res):
        js = json.loads(res)
        info = js['cards']
        fir = info[0]['card_group']
        sec = len(info[1]['card_group']) >= 3 and info[1]['card_group'] or info[2]['card_group']
        temp = []
        #地址
        temp.append(fir[2]['item_content'])
        #注册时间
        temp.append(sec[2]['item_content'])
        #信用
        temp.append(sec[1]['item_content'])
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

        elif (date.find(self.datesign) != -1) and len(date) <= 5:
            sign = date.find(self.datesign)
            month = date[ : sign]
            day = date[sign+1 : ]
            mday = '{}-{}-{}'.format(self.year, month, day)
            return mday

        else:
            return date

    def clean(self, text):
        emoList = self.emotion.findall(text)
        for e in emoList:
            text = self.emotion.sub(e, text, count = 1)
        text = self.tag.sub('', text)
        text = pm.escape_string(text)

        return text

    def getMblog(self, res):
        js = json.loads(res)
        js = js['cards']
        blog = []
        for num in range(len(js)):
            det = js[num]
            if 'card_group' in det.keys():
                continue
            det = det['mblog']

            if 'retweeted_status' in det.keys():
                temp = self.mblogDetail(det['retweeted_status'])
                blog.append(temp)

            temp = self.mblogDetail(det)
            blog.append(temp)

        del js
        blog = self.dic2list(blog)
        return blog

    def mblogDetail(self, blog):
        arr = {}
        if blog['user'] is None:
            return {}
        arr['id'] = blog['user']['id']
        arr['mid'] = blog['mid']
        arr['attitudes_count'] = blog['attitudes_count']
        arr['reposts_count'] = blog['reposts_count']
        arr['comments_count'] = blog['comments_count']
        arr['source'] = pm.escape_string(blog['source'])

        if 'retweeted_status' in blog.keys():
            arr['retweeted_status'] = blog['retweeted_status']['mid']
            text = blog['raw_text']
            text = text[ : text.find(self.raw_test)]
            text = self.tag.sub('', text)
            arr['text'] = pm.escape_string(text)
        else:
            arr['retweeted_status'] = 0
            arr['text'] = self.clean(blog['text'])

        if 'pics' in blog.keys():
            arr['pic_number'] = len(blog['pics'])
        else:
            arr['pic_number'] = 0

        arr['textLength'] = len(arr['text'])
        arr['create_at'] = self.getDate(blog['created_at'])

        return arr

    def dic2list(self, temp):
        res = []
        for blog in temp:
            if blog:
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
