#!/usr/bin/python
# -*- coding:utf-8 -*-

import sys
import MySQLdb
import CommInfo

reload(sys)
sys.setdefaultencoding('utf-8')


class MySql:
    __connectoin = None

    def __init__(self, *args):
        if len(args) == 2:
            self.__db, self.__user = args
            # info = getattr(__import__('CommInfo'), self.__db + '_' + self.__user)
            info = eval('CommInfo.' + self.__db + '_' + self.__user)
            self.__host, self.__user, self.__passwd = info['host'], info['user'], info['passwd']
        elif len(args) == 4:
            self.__host, self.__user, self.__passwd, self.__db = args
        else:
            raise ValueError
        
        self.__connect()

    def __del__(self):
        if (self.__connection):
            self.__connection.close()
            pass
        pass
    
    '''
    function: connect
    '''
    def __connect(self):
        try:
            self.__connection = MySQLdb.connect(
                host = self.__host,
                port = 3306,
                user = self.__user,
                passwd = self.__passwd,
                db = self.__db,
                charset = 'utf8')
        except MySQLdb.Error, e:
            print 'connect error: %s' % str()
            pass
        pass

    '''
    function: cursor
    '''
    def __cursor(self):
        try: 
            return self.__connection.cursor()
        except(AttributeError, MySQLdb.OperationalError):
            self.__connect()
            return self.__connection.cursor()

    '''
    执行sql，正常返回影响的行数，异常返回False
    '''
    def exeSql(self, sql):
        result = 0
        try:
            cursor = self.__cursor()
            result = cursor.execute(sql)
            self.__connection.commit()
        except MySQLdb.Error, e:
            print 'exeSql error: %s' % str(e)
            return False
        finally:
            if (cursor):
                cursor.close()
        return result

    '''
    批量执行sql，正常返回影响的行数，异常返回False
    '''
    def exeSqls(self, sql, args=None):
        result = 0
        try:
            cursor = self.__cursor()
            result = cursor.executemany(sql, args)
            self.__connection.commit()
        except MySQLdb.Error, e:
            print 'exeSqls error: %s' % str(e)
            return False
        finally:
            if (cursor):
                cursor.close()
        return result

    '''
    查询sql，正常返回查询的结果集，异常返回None
    '''
    def querySql(self, sql):
        result = []
        try:
            cursor = self.__cursor()
            cursor.execute(sql)
            records = cursor.fetchall()
            for record in records:
                result.append(record)
        except MySQLdb.Error, e:
            print 'querySql error: %s' % str(e)
            return None
        finally:
            if (cursor):
                cursor.close()
        return result

    '''
    查询一张表的所有字段名
    '''
    def getFields(self, table, db = None):
        result = []
        try:
            sql = 'select column_name from information_schema.columns \
                    where table_name = "%s" and table_schema = "%s"' % (table, db if db else self.__db)
            cursor = self.__cursor()
            cursor.execute(sql)
            records = cursor.fetchall()
            for record in records:
                result.append(record)
        except MySQLdb.Error, e:
            print 'querySql error: %s' % str(e)
            return None
        finally:
            if (cursor):
                cursor.close()
        return result

