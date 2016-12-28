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
import combineData as CD
import libLog as LG
import csv
import md5
import base64
import hashlib
import random

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def sortString(str):
    return ','.join(sorted(str.split(',')))
#    return ','.join(set(str.split(',')))
    
def haveNum(_s):
    if (not _s) or _s.strip()=='': return False
    st = set(_s)
    num = dict([[str(i), 1] for i in range(0,10)])
    for s in st:
        if s in num: return True
    return False   
    
def ttt():
    print 'tt'
    
def gtn():
    line = {'id':1}
    return line

def getCount(txt):
    ls = {}
    for c in txt:
        ls[c] = ls[c]+1 if c in ls else 1 
    #print sorted(ls.values())
    return ls

def getBase(src):
    s1 = base64.encodestring(src)
    s2 = base64.decodestring(s1)
    print '\t', s1
    print '\t', s2
    
def getMd5(src):
    print 
    m1 = md5.new()   
    m1.update(src)   
#    print m1.hexdigest()
#    print '\t', getCount(src)
#    print '\t', getCount(m1.hexdigest())
    
    return m1.hexdigest()


if __name__ == '__main__':
    print datetime.datetime.now()
#    ls = {}
#    lp = {}
#    for i in range(0, 10):
#        _tp = {'id':i, 'dt':i+2}
#        ls[i] = _tp
#        lp[i] = ls[i] 
#    ls[1]['dt'] = 12306
#    print ls
#    print lp
    
#    #1 - 10
#    s = ['s', 't', 'l']
#    p = "E8E9779F-0BDF-4869-ACC3-B6E7E8695F56"
#    print len(p), p[13:15]
#    if len(p)==36 and p[13:15]=='-4':
#        print 'ok'
        
#    for i in range(0, 100):
#        print s[int(random.random() * len(s))],
#    con1 = MongoClient('101.204.243.241', 27017)

#    ls = [ttt]
#    ls[0]()
#    tp1 = ['511026195601286014', '511026195603020615', '511026195603064415', '511026195401163714']
#    tp2 = ['17B5C712-7725-4149-8A05-901256AFCAFA', '00CED829-40DA-4A65-BE4B-E3B7542D60D9', '1DD94569-9DD2-448B-AB3B-A08B6E08F7BE',
#           '27D3B595-A311-4269-9D9B-81B1885EB25C']
#    index = 0
#    for t in tp1:
#        print t
#        print '\t', tp2[index]
#        
#        print '\t', getMd5(t[6:14] + t[0:6] + t[14:])
#        print '\t', getMd5(t[6:14] + t[14:] + t[0:6])
#        print '\t', getMd5(t[14:] + t[6:14] + t[0:6])
#        print '\t', getMd5(t[14:] + t[0:6] + t[6:14])
#        print '\t', getMd5(t[0:6] + t[14:] + t[6:14]) 
#        
#        getBase(t)
#        getBase(t[::-1])
#        print '\t', getMd5(t)
#        print '\t', getMd5(t[::-1])
#        print 
#        index += 1
#    sss = "AA52993F-4FEE-480F-AC82-7EFA7B19339F"
#    print getCount(sss)
#    src = '622623198807100039'; getMd5(src)
##    src = '622623'; getMd5(src)
##    src = '19880710'; getMd5(src)
##    src = '0039'; getMd5(src) 
#    print 
#    src = '622623198807100039'[::-1]; getMd5(src)
##    src = '622623'[::-1]; getMd5(src)
##    src = '19880710'[::-1]; getMd5(src)
##    src = '0039'[::-1]; getMd5(src)
#    src = '622623198807100039'; getMd5(src)
#    src = '622623198807100039'[::-1]; getMd5(src)
    
    
#    exit()    
    con1 = MongoClient('10.101.1.119', 27017)
    db = con1['jianzhu']
    ls = {}
    for item in db['personNew1'].find():
        if item['personId'] not in ls:
            ls[item['personId']] = item['company_name']
        else:
            print item['company_name'], ls[item['personId']]
    
    for item in db['personNew'].find():
        if item['personId'] not in ls:
            print item['company_name'], item['personId'], item['id']
    print len(ls)
    exit()
    
    for item in db['WCSafetyEngineer'].find():
#        if not P.checkIdCard(item['idCard']): 
            ls[item['idcard']] = item
#            print item['idCard']
    for item in db['personIDCard'].find():
#        if item['personIDCard'] in ls:
#                print item['personIDCard']
        if not P.checkIdCard(item['personIDCard']):
#            print item['personIDCard']
            if item['personIDCard'] in ls:
                print item['personIDCard']
    
#    db = con1['constructionDB']
#    db.authenticate("readWriteAny","abc@123","admin")
#    lg = LG.Log()
#    lg.log(1, 2, 3)

#    for item in db['bidding'].find({'type':'招标'}).sort('biddingDate',pymongo.DESCENDING).limit(100):
#        print item['biddingDate'], item['projectName']
#   
#    for item in db['gs_invitationBid'].find().sort('publishTime',pymongo.DESCENDING).limit(2):
#        print item['publishTime'], item
#        
#    for item in db['gst_project'].find().sort('publishDate',pymongo.DESCENDING).limit(2):
#        print item['publishDate'], item
    
             
#    print sortString("qwert,12345,asd,你妹啊,钢结构工程专,建筑装修装饰")
#    print sortString("qwert,你妹啊,asd,12345,建筑装修装饰,钢结构工程专")
    
#    reader = csv.reader(file(r'C:\Users\Administrator.xunying2\Desktop\test.csv', 'rb'))
#    for line in reader:
#        print line
#    print re.split('\.|·', 'hello.123·ffw你好')
    
    
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
#    print urllib.quote("百度")
#    ccode = 'fsnjn'
#    legal = None
#    _lsn = [haveNum(ccode), haveNum(legal), 1] 
#    _lsn1 = [haveNum(ccode), haveNum(legal), 1]; _lsn2 = [haveNum(ccode), haveNum(legal), 0]
#    pc = [ccode, legal, ''][[i for i in range(len(_lsn)) if _lsn1[i]][0]]
#    pl = [ccode, legal, ''][[i for i in range(len(_lsn)) if not _lsn2[i] and _lsn2[i]!=''][0]]
#    print [i for i in range(len(_lsn)) if not _lsn2[i] and _lsl[i]!='']
#    line = 'dda施工'.encode('utf8')
#    print line[-6:] 
#    if line[-6:]=='施工':
#        print line[:-6]
#    line = [['a', 1], ['b', 2]]
#    ls ={}
#    for li in line:
#        ls[li[0]] = []
#        l = li
#        ls[li[0]].append(l)
#    line = 2 
#    print ls
#    print datetime.date.today() - datetime.timedelta(days=1) 
#    s='2012-05-2'
    
#    sp = s.split('-')
##    print datetime.datetime(int(sp[0]), int(sp[1]), int(sp[2]))
#    print P.strToDate(s)
#    print P.dateToStr(P.strToDate(s))
#    print P.strToDate(s)
#    ls = [P.dateToStr(P.strToDate(s) + datetime.timedelta(days=i)) for i in range(1, 5)]
#    ls += [P.dateToStr(P.strToDate(s) - datetime.timedelta(days=i)) for i in range(1, 5)]
#    for l in ls:
#        print l
    
#    a = 12
#    tp()
#    print a
#    ls = []
#    for i in range(5):
#        ln =gtn()
#        ls.append(ln)
#    ls[0]['id'] = 32
#    print ls
#    st = ("23"
#        "13"'23')
    
#    print pc
#    print pl
#    print re.sub(str(None), '', str("sads"))
    
#    print [i for i in range(len(_lsn)) if _lsn[i]]    
#    print code
#    parse.quote('百度')
    
#    ls = [{"s":2}, {"f":3}, {"w":21}]
#    ll = []
#    for l in ls:
#        ll.append(l)
#    ls.remove({"f":3})
#    ls[0]['s'] = 18
#    print ls
#    print ll
#    ls = []
#    print ll
#    con1 = MongoClient('localhost', 27017)
#    con2 = MongoClient('192.168.3.45', 27017)
#    con3 = MongoClient('171.221.173.154', 27017)
#    con1 = MongoClient('192.168.3.119', 27017)
#    db1 = con1['jianzhu3']
#    print db1['bidding'].find().count()
    
#    tb = db1.tbtest
#    lines = [{'cpname':'公司1', 'proj':'项目1', 'type':'中标'},
#             {'cpname':'公司1', 'proj':'项目2', 'type':'中标'},
#             {'cpname':'公司1', 'proj':'项目3', 'type':'中标'},
#             {'cpname':'公司2', 'proj':'项目4', 'type':'中标'},
#             {'cpname':'', 'proj':'项目5'},
#             {'cpname':'', 'proj':'项目1'},
#             {'cpname':'', 'proj':'项目6'},
#             ]
#    tb.insert(lines)
#    
#    for line in tb.find({}):
#        print line['cpname']
#    reducer = """
#                   function(obj, prev){
#                       prev.count++;
#                   }
#            """

#    results = tb.group(key={"cpname":1}, condition={'type':'中标', '':{'$':''}}, initial={"count": 0}, reduce=reducer)
#
#    for s in results:
#        print s['cpname'], s['count']
#    for line in tb.find():
#        print line
#    {'cpname':{'$gt':''}}        
#              
#              
              
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