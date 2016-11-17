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
    
def haveNum(_s):
    if (not _s) or _s.strip()=='': return False
    st = set(_s)
    num = dict([[str(i), 1] for i in range(0,10)])
    for s in st:
        if s in num: return True
    return False   
    
def tp():
    print a

def gtn():
    line = {'id':1}
    return line

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
    st = ("23"
        "13"'23')
    
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
    con1 = MongoClient('192.168.3.119', 27017)
    db1 = con1['jianzhu3']
    print db1['bidding'].find().count()
    
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