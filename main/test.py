#coding:utf8
'''
Created on Jul 18, 2016

@author: Administrator
'''
from pymongo import MongoClient
import re
import datetime
import pymongo
from pymongo import MongoClient
import time
import public as P
import urllib

def test():
    f=2
if __name__ == '__main__':
    print datetime.datetime.now()
    #    re.findall()
#    cs = re.compile(r'\d{4}\.\d{2} - \d{4}\.\d{2}|\d{4}\.\d{2} - 至今')
#    test = '2013.02 - 2014.23上的伤口'
#    st = '打卡哪款的那款的.csF24B7080{text-align:left;text-indent:21pt;margin:0pt0pt0pt0pt}送分分分'
#    print cs.sub('a', st)
#    test = '2013.02 - 2014.12年左右'
#    test = '工作描述：    负责公司OA办公软件界面设计及制作负责公司宣传册 EDM 及其他宣传物的设计制作 '
#    cs = re.compile(r'(?=.cs)([\s\S]*)(?<=})')
#    print re.sub(r'(?=.cs)([\s\S]*)(?<=})', '', st)
#    print cs.findall(st)[0]
#    s = re.sub(r'(?=.cs)([\s\S]*)(?<=}|\t|\n|([\s+]))', '', '            ')+'2'
#    s = re.sub(r'(?=.cs)([\s\S]*)(?<=}|\t|\n|[\s]', '', '            ')+'2'
#    print re.sub(r'(?={)([\s\S]*)(?<=})', '', 'span.15{font-family:Calibri;font-weight:bold;text-decoration:underline;text-underline:single;}')
#    s = 'span.15{font-family:Calibri;font-weight:bold;text-decoration:underline;text-underline:single;}\n' + 'span.16{font-family:宋体;font-weight:bold;text-decoration:underline;text-underline:single;font-size:12.0000pt;}'
#    print s
#    print re.sub(r'(?={)([\s\S]*)(?<=})|(?=\/\*)([\s\S]*)(?<=\*\/)', '', 'dada/*:eWebEditor*/qq') 
#    print re.findall(r'(?=<tr)([\s\S]+?)(?<=<\/tr>){2}', '<tr>Hello</tr><tr>Moto</tr><tr>Hao</tr>')
#    if 0: print 2; print 3
#    s = 1
#    print 1 ^ s
#    st ='无/&nbsp;2'
#    price = re.sub('无|/|&nbsp;', '', st)
#    print price
#    print re.sub('无|/|&nbsp;', '', '1/'.strip().encode('utf8')).strip()
#    st = '12.34 '.encode('utf8').strip()
#    s = re.sub(' ', '', st)
#    s = st.replace(' ', '')
#    print float(s)
#    st={'k1':12, 'k2':3}
#    ss = {'k3':4, 'k4':6}
#    st+=ss
#    print st
    print urllib.quote("百度")
#    parse.quote('百度')
    
#    con1 = MongoClient('localhost', 27017)
#    con2 = MongoClient('192.168.3.45', 27017)
#    con3 = MongoClient('171.221.173.154', 27017)
#    db1 = con1['middle']
#    db2 = con2['constructionDB']
#    db3 = con3['jianzhu3']
#    index = 0
#    person = ''
#    print P.getMaxId(db1.companyInfoNew2, 'id')
#    lsComp = {}
#    for item in db1.personNew.find():
#        cpname = item['companyname']
#        for ln in item['certificate']:
#            if ln['name']=='注册建造师' and ln['professional']=='机电工程':
#                if cpname not in lsComp: lsComp[cpname] = 1
#    print len(lsComp)
    
#    for item in db1.personNew1.find():
#        if len(item['certificate'])>index: 
##            if 50442891==item['id'] or 50688791==item['id']: continue
#            person = item['id']
#            index = len(item['certificate'])
#    print index, person   
#    print ' '=='\r'
#    print 1
    
#    print re.sub('\s\S', ' \n    ')
    
#    print len(s)
    
    
#    la = re.sub('.cs/*', '2', st)
#    print la
#    return
    
#    st = "你好-223 都是到 ddsd"
#    print re.sub('-| ', '', st)
    
#    print re.split('、|\ ', '水工建筑、机电设备安装、环境保护、机电设备制造、金属结构设备制造')
#    for ln in re.split('、|\ ', '水工建筑 机电设备安装、环境保护、机电设备制造、金属结构设备制造'):
#        print ln
    
#    ls = []
#    for i in range(0,10):
#        t = []
#        t.append(i)
#        ls.append(t)
#    print ls
    
#    tp = {'寤�:'鍦熷缓', '姘�:'姘村埄', '璺�:'鍏矾'}  #Code_F
#    print tp
#    print ['受伤的是你']
    
#    list1 = ['2', '4', '7']
#    print ','.join(list1)
#    
#    exit() 
#    con = MongoClient('localhost', 27017)
#    db2 = con['middle']
#    write = db2.person
#    ls = write.find({}) 
#    for l in ls:
#        if l['companyname']>1:
#            for c in l['companyname']:
#                print c
#    print ls.count()
    
#    print ls[hello]
    
#                n['professionalType'] = []
#                for p in companyDic[l]['qualificationType'][n['name']].keys():
#                    n['professionalType'].append({'name':p, 'professionalLevel':companyDic[l]['qualificationType'][n['name']][p].keys()})