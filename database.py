# -*- coding: utf-8 -*-
# =============================================================================
# 更新说明：
# 将个别函数出错的sql全都输出到文件中，方便查看具体错误
# 对getInfo()进行逻辑完善，使其在返回用户信时，将redundance置为4，保证当下不被反复抓取
# ！！！此处存在插入关注信息时会出现被关注者ID为1的情况！！！
# =============================================================================
# TODO: 所有操作失败后的处理方式
# TODO: 各操作类需要加锁
import pymysql as pm
from middle.settings import MYSQL_DB, MYSQL_HOSTS, MYSQL_USER, MYSQL_PW, MYSQL_PORT
from middle.settings import DB_USERINFO, DB_MBLOGINFO, DB_FOLINFO, DB_FANINFO, DB_PROXY

class SqlOpe:
    def __init__(self, user, pw, table, hosts = MYSQL_HOSTS, port = MYSQL_PORT, db = MYSQL_DB):
        self.ope = pm.connect(host = hosts, 
                              port = port,
                              db = db,
                              user = user,
                              password = pw,
                              charset = 'utf8')
        self.table = table
        self.cursor = self.ope.cursor()  
        self.cursor.execute('use {}'.format(db))      

    def findUserId (self, uid):
        sql = 'select `userid` from `{}` where `userid` = {}'.format(self.table, uid)
        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        if res is None:
            return None
        else:
            return res[0]

    def removeUser(self, uid):
        #只删除一个会出问题，但是该爬虫不会出现只删除一次的情况，故不需要更多考虑
        uid = str(tuple(uid))
        sql = 'DELETE FROM `{}` WHERE `userid` IN '.format(self.table) + uid
        res = self.cursor.execute(sql)
        self.ope.commit()
        return res

    def close(self):
        self.cursor.close()
        self.ope.close()  

# `redundance` = 0 代表该用户ID仅存有ID
#              = 1 代表该用户已完善第一步信息
#              = 2 代表该用户已经完成爬取
#              = 3 代表该用户粉丝数微博数大于3000，只记录信息，不对其进行爬取
#              = 4 代表该用户正在被爬取
class UserOpe (SqlOpe):
    def __init__(self, user = MYSQL_USER, pw = MYSQL_PW, table = DB_USERINFO, 
                 hosts = MYSQL_HOSTS, port = MYSQL_PORT, db = MYSQL_DB):
        SqlOpe.__init__(self, user, pw, table, hosts, port, db)

#插入初步用户信息
    def insert (self, info):
        sql = 'INSERT INTO `{}`(`userid`, `statuses_count`,\
                `fans_count`, `follow_count`, `urank`, `username`, \
                `description`, `gender`, `verified_reason`, `redundance`) VALUES'.format(self.table)

        num = ' %s,' * 5
        char = ' \'%s\',' * 4

        if (info[2] > 3000 or info[3] > 3000):
            end = ' ON DUPLICATE KEY UPDATE `redundance` = 3'
            val = ' (' + num[1:] + char + ' 3)'
        else:
            end = ' ON DUPLICATE KEY UPDATE `redundance` = 1'
            val = ' (' + num[1:] + char + ' 1)'

        val = val % tuple(info)
        sql = sql + val + end
        try:
            self.cursor.execute(sql)
        except:
            with open('insertwrong.txt', 'a+') as f:
                f.write(sql)
            print (sql)
        self.ope.commit()

# 完善用户所有信息
    def update(self, uid, info):
        sql = 'UPDATE `{}` SET `address` = \'{}\', `createtime` = \'{}\', `credit` = \'{}\',\
                    `utag` = \'{}\', `redundance` = 2 WHERE `userid` = {}'
        sql = sql.format(self.table, info[0], info[1], info[2], info[3], uid)
        try:
            self.cursor.execute(sql)
            self.ope.commit()
        except:
            self.ope.rollback()

#获取用户ID，用于完善信息
    def getId(self, num):
        sql = 'SELECT `userid` FROM `{}` WHERE `redundance` = 0 LIMIT '.format(self.table) + str(num)
        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        if res is None:
            return None
        else:
            uid = []
            for n in res:
                uid.append(n[0])
            print (uid)
            if uid.__len__() != 0:
                self.removeUser(uid)
            return uid

#获取用户信息用于后续爬取
    def getInfo(self):
        sql = 'SELECT `userid`, `statuses_count`, `fans_count`, `follow_count` \
        FROM `{}` WHERE `redundance` = 1 LIMIT 1'.format(self.table)

        self.cursor.execute(sql)
        res = self.cursor.fetchone()

        if res is None:
            return None
        else:
            sql = 'UPDATE {} SET `redundance` = 4 WHERE `userid` = {}'.format(self.table, res[0])
            self.cursor.execute(sql)
            self.ope.commit()
            return res

class MblogOpe (SqlOpe):
    def __init__(self, user = MYSQL_USER, pw = MYSQL_PW, table = DB_MBLOGINFO, 
                 hosts = MYSQL_HOSTS, port = MYSQL_PORT, db = MYSQL_DB):
        SqlOpe.__init__(self, user, pw, table, hosts, port, db)

    def insert(self, info):
        sql = 'INSERT INTO `{}` VALUES'.format(self.table)
        det = []
        end = ' ON DUPLICATE KEY UPDATE `uid` = `uid` AND `mid` = `mid`'
        for item in info:
            num = ' %s,' * 8
            char = ' \'%s\',' * 3
            val = ' (' + num[1:] + char[:-1] + ')'
            val = val % tuple(item)
            det.append(val)
        val = ','.join(det)
        sql = sql + val + end
        try:
            self.cursor.execute(sql)
        except:
            with open('mblogwrong.txt', 'a+') as f:
                f.write(sql)
        self.ope.commit()

# =============================================================================
# 数据插入之后会提取一份到UserDatabase
# =============================================================================
class FanOpe (SqlOpe):
    def __init__(self, user = MYSQL_USER, pw = MYSQL_PW, table = DB_FANINFO, 
                 hosts = MYSQL_HOSTS, port = MYSQL_PORT, db = MYSQL_DB):
        SqlOpe.__init__(self, user, pw, table, hosts, port, db)

    def insert(self, hoster, uid):
        sql = 'INSERT INTO `{}` VALUES'.format(self.table)
        end = ' ON DUPLICATE KEY UPDATE `uid` = `uid` AND `fan_id` = `fan_id`'
        val = ' ({}, %s),'.format(hoster)
        val = val * len(uid) % tuple(uid)
        sql = sql + val[:-1] + end
        self.cursor.execute(sql)
        self.ope.commit()

class FolOpe (SqlOpe):
    def __init__(self, user = MYSQL_USER, pw = MYSQL_PW, table = DB_FOLINFO, 
                 hosts = MYSQL_HOSTS, port = MYSQL_PORT, db = MYSQL_DB):
        SqlOpe.__init__(self, user, pw, table, hosts, port, db)

    def insert(self, hoster, uid):
        sql = 'INSERT INTO `{}` VALUES'.format(self.table)
        end = ' ON DUPLICATE KEY UPDATE `uid` = `uid` AND `follow_id` = `follow_id`'
        val = ' ({}, %s),'.format(hoster)
        val = val * len(uid) % tuple(uid)
        sql = sql + val[:-1] + end
        self.cursor.execute(sql)
        self.ope.commit()

class proxyOpe(SqlOpe):
    def __init__(self, user = MYSQL_USER, pw = MYSQL_PW, table = DB_PROXY, 
                 hosts = MYSQL_HOSTS, port = MYSQL_PORT, db = MYSQL_DB):
        SqlOpe.__init__(self, user, pw, table, hosts, port, db)

    def delIp(self, ip):
        sql = 'DELETE FROM `{}` WHERE '
        sql = 'DELETE FROM `{}` WHERE `ip` = \''.format(self.table)
        sql = sql + ip + '\''
        self.cursor.execute(sql)
        self.ope.commit()
    
    def getProxy(self):
        sql = 'SELECT `ip`, `port` FROM `{}` LIMIT 1'.format(self.table)
        self.cursor.execute(sql)
        res = self.cursor.fetchone()
        if res is None:
            return None
        else:
            return res

if __name__ == "__main__":
    url = UserOpe()
    print (url.getId(1))
