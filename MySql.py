#!/usr/bin/python
# -*- coding:utf-8 -*-

import sys
import MySQLdb

reload(sys)
sys.setdefaultencoding('utf-8')

class MySql:
    __connect, __cursor = None, None

    def __init__(self, host, user, passwd, db):
        try:
            self.__connect = MySQLdb.connect(
                    host = host,
                    port = 3306,
                    user = user,
                    passwd = passwd,
                    db = db,
                    charset = 'utf8'
                )
            self.__cursor = self.__connect.cursor()
            self.__db = db
        except MySQLdb.Error, e:
            print 'connect error: %s' % str()
    

    def __del__(self):
        if (self.__cursor):
            self.__cursor.close()
        if (self.__connect):
            self.__connect.close()
    
    
    '''
    执行sql，正常返回影响的行数，异常返回False
    '''
    def exeSql(self, sql):
        result = 0
        try:
            self.__cursor = self.__connect.cursor()
            result = self.__cursor.execute(sql)
            self.__connect.commit()
        except MySQLdb.Error, e:
            print 'exeSql error: %s' % str(e)
            return False
        finally:
            if (self.__cursor):
                self.__cursor.close()
        return result

    '''
    批量执行sql，正常返回影响的行数，异常返回False
    '''
    def exeSqls(self, sql, args = None):
        result = 0
        try:
            self.__cursor = self.__connect.cursor()
            result = self.__cursor.executemany(sql, args)
            self.__connect.commit()
        except MySQLdb.Error, e:
            print 'exeSqls error: %s' % str(e)
            return False
        finally:
            if (self.__cursor):
                self.__cursor.close()
        return result

    '''
    查询sql，正常返回查询的结果集，异常返回None
    '''
    def querySql(self, sql):
        result = []
        try:            
            self.__cursor = self.__connect.cursor()
            self.__cursor.execute(sql)
            records = self.__cursor.fetchall()
            for record in records:
                result.append(record)
        except MySQLdb.Error, e:
            print 'querySql error: %s' % str(e)
            return None
        finally:
            if (self.__cursor):
                self.__cursor.close()
        return result

    '''
    查询一张表的所有字段名
    '''
    def getFields(self, table, db = None):   
        result = []
        try:
            sql = 'select column_name from information_schema.columns \
                    where table_name = "%s" and table_schema = "%s"' % (table, db if db else self.__db)
            self.__cursor = self.__connect.cursor()
            self.__cursor.execute(sql)
            records = self.__cursor.fetchall()
            for record in records:
                result.append(record)
        except MySQLdb.Error, e:
            print 'querySql error: %s' % str(e)
            return None
        finally:
            if (self.__cursor):
                self.__cursor.close()
        return result


