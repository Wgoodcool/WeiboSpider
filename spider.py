# -*- coding: utf-8 -*-
"""
Created on Mon Oct  9 08:57:48 2017

@author: wzx0518
"""
# =============================================================================
# TODO: 数据库操作类需要考虑并发问题,数据库操作类需要加锁
# =============================================================================
from middle.middlequeue import responseQueue, userResponseQueue
from manager import InfoManager

from multiprocessing import Process
import json
import re
import datetime

class WeiboSpider:
    def Start(self):
        com = Process(target = self.CommonSpider, args = (responseQueue, ))
        user = Process(target = self.UserSpider, args = (userResponseQueue, ))

        com.start()
        user.start()

        com.join()
        user.join()

    def CommonSpider(self, queue):
        self.manager = InfoManager()
        self.manager.start()
# 分别用于转换表情，去除HTML标签，提取数字
        self.emotion = re.compile('<span.*?url-icon.*?alt="(.*?)">.*?</span>')
        self.tag = re.compile(r'<[^>]+>',re.S)
        self.number = re.compile('\D')
#用于匹配日期
        self.minutes = '分钟'
        self.hour = '小时'
#    数据库操作类
        
        self.db_fan = self.manager.FanOpe()
        self.db_fol = self.manager.FolOpe()
        self.db_mb = self.manager.MblogOpe()
        print (type(self.db_fan))

        res = queue.get()
        while res:
            if res.category == 1:
                result = self.getDetail(res.text)
                self.db_user.update(result)

            elif res.category == 2:
                result = self.getFan(res.text)
                self.db_fan.insert(res.meta['uid'], result)

            elif res.category == 3:
                result = self.getFol(res.text)
                self.db_fol.insert(res.meta['uid'], result)

            else:
                result = self.getMblog(res.text)
                temp = self.getMblog(result)
                self.db_mb.insert(temp)

    def UserSpider(self, queue):
        self.manager = InfoManager()
        self.manager.start()
        self.db_user = self.manager.UserOpe()
        
        res = queue.get()
        while res:
            if res.category == 0:
                result = self.getUserInfo(res.text)
                if result[2] <= 3000 or result[3] <= 3000:
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
        temp.append(user['urank'])
        temp.append(user['screen_name'])
        temp.append(user['description'])
        temp.append(user['gender'])
        temp.append(user['verified_reason'])
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
        else:
            return date

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
            else:
                arr['retweeted_status'] = -1

            if 'pics' in det.keys():
                arr['pic_number'] = len(det['pics'])
            else:
                arr['pic_number'] = 0
        
            arr['text'] = self.clean(det['text'], arr['retweeted_status'])
            arr['textLength'] = len(arr['text'])
            arr['create_at'] = self.getDate(det['created_at'])
            arr['picture_road'] = 'None'
            
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
