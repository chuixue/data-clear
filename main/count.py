#encoding:utf8
'''
Created on Sep 1, 2016

@author: Administrator
'''
import pymongo
from pymongo import MongoClient
import time
import datetime
import re


def cout(ls):
    for l in ls: print l, ':', ls[l],
    print
def out(ls):
    for l in ls: print l,
    print

def select1():
    con1 = MongoClient('localhost', 27017)
    con2 = MongoClient('192.168.3.45', 27017)
    con3 = MongoClient('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    
    print db1.companyInfoNew2.find().count()


#统计
def select2():
    con = MongoClient('localhost', 27017)
    db2 = con['middle']
    person = db2.person
    company = db2.companyInfo
    
    keyp = ['注册建造师', '安考证', '造价工程师', '造价员', '注册安全工程师']
    for k in keyp: print k, ': ', person.find({'certificate.name':k }).count()
    #打印同时多个证人数矩阵
    print '\t',
    for k in keyp: print k,'\t',
    print
    for i in range(len(keyp)):
        print keyp[i],'\t',
        for j in range(len(keyp)): print person.find({'$and':[{'certificate.name':keyp[i]},{'certificate.name':keyp[j]}]}).count(),'\t',
        print
    
    return
    cIndex = 0
    bIndex = 0
    gIndex = 0
    qIndex = 0
    for item in company.find():
        if 'courtRecords' in item and len(item['courtRecords'])>0: cIndex += 1
        if len(item['badbehaviors']['badBehaviorDetails'])>0: bIndex += 1
        if len(item['goodbehaviors'])>0: gIndex += 1
        if len(item['qualificationType'])>0: qIndex += 1
    print '诉讼记录', cIndex    
    print '不良记录', bIndex    
    print '优良记录', gIndex
    print '公司资质', qIndex
    print '建造师', company.find({'certificate.type':'建造师'}).count()
    print '无资质公司', company.find({'qualificationType':[]}).count()    



#输出公司表数据详情
def selectCompany():
    con1 = MongoClient('localhost', 27017)
    db1 = con1['middle']
    lst = {}
    for item in db1.companyInfoNew.find():
        for q in item['qualification']:
            if q['type'] not in lst: lst[q['type']] = {} 
            if q['class'] not in lst[q['type']]: lst[q['type']][q['class']] = {}
            if q['professional'] not in lst[q['type']][q['class']]: lst[q['type']][q['class']][q['professional']] = {}
            if q['level'] not in lst[q['type']][q['class']][q['professional']]: lst[q['type']][q['class']][q['professional']][q['level']] = 0
            lst[q['type']][q['class']][q['professional']][q['level']] += 1
            pass
        
    for t in lst:
        print t, ':'
        for c in lst[t]:
            print '\t', c, ':'
            for p in lst[t][c]:
                print '\t\t', p, ':',
                for l in lst[t][c][p]:
                    print l, '', lst[t][c][p][l], ',',
                print   
                
#输出人员表数据详情        
def selectPerson():
    con = MongoClient('localhost', 27017)
    db1 = con['middle']
    lst = {}
    for item in db1.personNew.find():
        for c in item['certificate']:
            if c['name'] not in lst: lst[c['name']] = {} 
            if c['professional'] not in lst[c['name']]: lst[c['name']][c['professional']] = {}
            if c['level'] not in lst[c['name']][c['professional']]: lst[c['name']][c['professional']][c['level']] = 0
            lst[c['name']][c['professional']][c['level']] += 1
    for n in lst:
        print n, ':'
        for p in lst[n]:
            print '\t\t', p, ':',
            for l in lst[n][p]:
                print l, '', lst[n][p][l], ',',
            print   
                
if __name__ == '__main__':
#    select1()
    selectCompany()
#    selectPerson()
    
    
    pass