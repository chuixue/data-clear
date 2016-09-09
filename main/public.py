#coding:utf8
'''
Created on Sep 9, 2016

@author: Administrator
'''
def dbKeys(table, keys):
    temp = dict((k, 1) for k in keys)
    for item in table.find({}).limit(1): lskey = dict((key, 0) for key in item if key not in temp)
    return lskey

def getCompanyId(table):
    return dict((item['company_name'].encode('utf8'), 
                 item['id']) for item in table.find({}, dbKeys(table, ['company_name', 'id']))) 

 