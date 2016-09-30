#coding:utf8
'''
Created on Sep 30, 2016

@author: Administrator
'''
import datetime
import mysql.connector

cnx = mysql.connector.connect(user='root', password='root', database='dianjian', host='192.168.3.177')
cursor = cnx.cursor()

'''插入'''
sql = ("INSERT INTO md_person (personId, name, unit, place, telephone, updateTime, mark) VALUES ("
         '%s, %s, %s, %s, %s, %s, %s)')
sdt = ('10006', 'David', '项目部', '成都', '13724056231', str(datetime.datetime.now()), '测试数据')
cursor.execute(sql, sdt)
cnx.commit()

'''查询'''
sql = "select personId, name, unit, place, telephone from md_person where 1"
cursor.execute(sql)
for (id ,name, unit, place, tel) in cursor:
    print id, name, unit.encode('utf8'), tel

cursor.close()
cnx.close()
