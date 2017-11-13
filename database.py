# -*- coding: utf-8 -*-
import pymysql as pm
from middle.settings import MYSQL_DB, MYSQL_HOSTS, MYSQL_USER, MYSQL_PW, MYSQL_PORT
from middle.settings import DB_USERINFO, DB_MBLOGINFO, DB_FOLINFO, DB_FANINFO, DB_PROXY

from warnings import filterwarnings
filterwarnings('error', category=pm.Warning)

class SqlOpe:
    def __init__(self, user, pw, table, hosts = MYSQL_HOSTS, port = MYSQL_PORT, db = MYSQL_DB):
        self.ope = pm.connect(host = hosts, 
                              port = port,
                              db = db,
                              user = user,
                              password = pw,
                              charset = 'utf8mb4')
        self.table = table
        self.cursor = self.ope.cursor()  
        self.cursor.execute('use {}'.format(db)) 
        # 保证数据库连接是utf8mb4，以支持Unicode emoji 
        self.cursor.execute('SET NAMES utf8mb4;')     

    def findUserId (self, uid):
        sql = 'select `userid` from `{}` where `userid` = {}'.format(self.table, uid)
        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        if res is None:
            return None
        else:
            return res[0]

    def removeUser(self, uid):
        if len(uid) == 1:
            uid = '({})'.format(uid[0])
        else:
            uid = str(tuple(uid))

        sql = 'DELETE FROM `{}` WHERE `userid` IN '.format(self.table) + uid
        try:
            res = self.cursor.execute(sql)

        except pm.Warning as w:
            with open('db_remove_warning.txt', 'a+') as f:
                f.write('Warning :')
                f.write(repr(w))
                f.write('\n')

        except pm.Error as e:
            try:
                errorInfo = "Error %d:%s" % (e.args[0], e.args[1])
            except IndexError:
                errorInfo = "MySQL Error: %s" % str(e)

            with open('db_remove.txt', 'a+') as f:
                f.write(sql)
                f.write('\n')
                f.write(errorInfo)
                f.write('\n\n')

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
    def insert (self, info, url):
        sql = 'INSERT IGNORE INTO `{}`(`userid`, `statuses_count`,\
                `fans_count`, `follow_count`, `urank`, `username`, \
                `description`, `gender`, `verified_reason`, `redundance`) VALUES'.format(self.table)

        num = ' %s,' * 5
        char = ' \'%s\',' * 4

        if info[2] > 3000 or info[3] > 3000:
            val = ' (' + num[1:] + char + ' 3)'
        else:
            val = ' (' + num[1:] + char + ' 1)'

        val = val % tuple(info)
        sql = sql + val
        try:
            self.cursor.execute(sql)

        except pm.Warning as w:
            with open('db_user_insert_warning.txt', 'a+') as f:
                f.write('Warning :')
                f.write(repr(w))
                f.write('\n')
                f.write(url)
                f.write('\n\n')

        except pm.Error as e:
            with open('db_user_insert.txt', 'a+') as f:
                f.write(url)
                f.write('\n')
                f.write(sql)
                f.write('\n')
                f.write(repr(e))
                f.write('\n\n')
        else:
            self.ope.commit()

# 完善用户所有信息
    def update(self, uid, info, url):
        sql = 'UPDATE `{}` SET `address` = \'{}\', `createtime` = \'{}\', `credit` = \'{}\',\
                     `redundance` = 2 WHERE `userid` = {}'
        sql = sql.format(self.table, info[0], info[1], info[2], uid)
        try:
            self.cursor.execute(sql)

        except pm.Warning as w:
            with open('db_user_update_warning.txt', 'a+') as f:
                f.write('Warning :')
                f.write(repr(w))
                f.write('\n')
                f.write(url)
                f.write('\n\n')

        except pm.Error as e:
            self.ope.rollback()
            with open('db_user_update.txt', 'a+') as f:
                f.write(url)
                f.write('\n')
                f.write(sql)
                f.write('\n')
                f.write(repr(e))
                f.write('\n\n')
        else:
            self.ope.commit()

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
            try:
                sql = 'UPDATE {} SET `redundance` = 4 WHERE `userid` = {}'.format(self.table, res[0])
                self.cursor.execute(sql)
            except pm.Warning as w:
                with open('db_user_getInfo_warning.txt', 'a+') as f:
                    f.write('Warning :')
                    f.write(repr(w))
                    f.write('\n')
            except pm.Error as e:
                self.ope.rollback()
                try:
                    errorInfo = "Error %d:%s" % (e.args[0], e.args[1])
                except IndexError:
                    errorInfo = "MySQL Error: %s" % str(e)
                with open('db_user_getinfo.txt', 'a+') as f:
                    f.write(url)
                    f.write('\n')
                    f.write(sql)
                    f.write('\n')
                    f.write(errorInfo)
                    f.write('\n\n')
            else:
                self.ope.commit()
                return res

class MblogOpe (SqlOpe):
    def __init__(self, user = MYSQL_USER, pw = MYSQL_PW, table = DB_MBLOGINFO, 
                 hosts = MYSQL_HOSTS, port = MYSQL_PORT, db = MYSQL_DB):
        SqlOpe.__init__(self, user, pw, table, hosts, port, db)

    def insert(self, info, url):
        sql = 'INSERT IGNORE INTO `{}` VALUES'.format(self.table)
        det = []
        for item in info:
            num = ' %s,' * 8
            char = ' \'%s\',' * 3
            val = ' (' + num[1:] + char[:-1] + ')'
            val = val % tuple(item)
            det.append(val)
        val = ','.join(det)
        sql = sql + val

        try:
            self.cursor.execute(sql)

        except pm.Warning as w:
            with open('db_mblog_insert_warning.txt', 'a+') as f:
                f.write('Warning :')
                f.write(repr(w))
                f.write('\n')
                f.write(url)
                f.write('\n\n')

        except pm.Error as e:
            with open('db_mblog_insert.txt', 'a+') as f:
                f.write(url)
                f.write('\n')
                f.write(sql)
                f.write('\n')
                f.write(repr(e))
                f.write('\n\n')
        else:
            self.ope.commit()

# =============================================================================
# 数据插入之后会提取一份到UserDatabase
# =============================================================================
class FanOpe (SqlOpe):
    def __init__(self, user = MYSQL_USER, pw = MYSQL_PW, table = DB_FANINFO, 
                 hosts = MYSQL_HOSTS, port = MYSQL_PORT, db = MYSQL_DB):
        SqlOpe.__init__(self, user, pw, table, hosts, port, db)

    def insert(self, hoster, uid, url):
        sql = 'INSERT IGNORE INTO `{}` VALUES'.format(self.table)
        val = ' ({}, %s),'.format(hoster)
        val = val * len(uid) % tuple(uid)
        sql = sql + val[:-1]

        try:
            self.cursor.execute(sql)

        except pm.Warning as w:
            with open('db_fan_insert_warning.txt', 'a+') as f:
                f.write('Warning :')
                f.write(repr(w))
                f.write('\n')
                f.write(url)
                f.write('\n\n')

        except pm.Error as e:
            with open('db_fan_insert.txt', 'a+') as f:
                f.write(url)
                f.write('\n')
                f.write(sql)
                f.write('\n')
                f.write(repr(e))
                f.write('\n\n')
        else:
            self.ope.commit()

class FolOpe (SqlOpe):
    def __init__(self, user = MYSQL_USER, pw = MYSQL_PW, table = DB_FOLINFO, 
                 hosts = MYSQL_HOSTS, port = MYSQL_PORT, db = MYSQL_DB):
        SqlOpe.__init__(self, user, pw, table, hosts, port, db)

    def insert(self, hoster, uid, url):
        sql = 'INSERT IGNORE INTO `{}` VALUES'.format(self.table)
        val = ' ({}, %s),'.format(hoster)
        val = val * len(uid) % tuple(uid)
        sql = sql + val[:-1]

        try:
            self.cursor.execute(sql)

        except pm.Warning as w:
            with open('db_follow_insert_warning.txt', 'a+') as f:
                f.write('Warning :')
                f.write(repr(w))
                f.write('\n')
                f.write(url)
                f.write('\n\n')

        except pm.Error as e:
            with open('db_follow_insert.txt', 'a+') as f:
                f.write(url)
                f.write('\n')
                f.write(sql)
                f.write('\n')
                f.write(repr(e))
                f.write('\n\n')
        else:
            self.ope.commit()

if __name__ == "__main__":
    fan = FanOpe()
    fan.insert(123, [111, 123], 'aaa')
