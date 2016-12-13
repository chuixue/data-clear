#coding:utf8
'''
Created on Sep 9, 2016

@author: Administrator
'''
import datetime
import re
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def cout(ls):
    for l in ls: print l, ':', ls[l] if type(ls[l])!=type('') else ls[l].encode('utf8'),
    print
def out(ls):
    for l in ls: print l,
    print

def dbKeys(table, keys):
    temp = dict((k, 1) for k in keys)
    lskey = {}
    for item in table.find({}).limit(1): lskey = dict((key, 0) for key in item if key not in temp)
    return lskey

def getCompanyId(table):
    return dict((item['company_name'].encode('utf8'), 
                 item['id']) for item in table.find({}, dbKeys(table, ['company_name', 'id']))) 

def getMaxId(table, name):
    if len(dbKeys(table, [name]))==0: return -1 
    return table.find({}, dbKeys(table, [name])).sort(name, -1).limit(1)[0][name]

def strToDate(_str):
    try:
        sp = _str.strip().split('-')
        dt = datetime.datetime(int(sp[0]), int(sp[1]), int(sp[2]))
        return dt
    except:
        return None
 
def dateToStr(_date):
    if not _date: return ''
    return _date.strftime('%Y-%m-%d').encode('utf8')

def dateFormat(_str):
    return dateToStr(strToDate(_str)) 

def haveNum(_s):
    if (not _s) or _s.strip()=='': return False
    st = set(_s)
    num = dict([[str(i), 1] for i in range(0,10)])
    for s in st:
        if s in num: return True
    return False

def Date_F(str):
    if not str or str == '': return ''
    sp = []
    if str.find("年")>0 and str.find("月")>0: 
        sp = re.split("年|月".decode('utf8'), str)
    elif str.find("-")>0:
        sp = re.split("-".decode('utf8'), str)
    if len(sp)!=3: return str
    try:
        _dt = datetime.datetime(int(sp[0]), int(sp[1]), int(sp[2].replace('日', '')))
        _dts = _dt.strftime('%Y-%m-%d').encode('utf8')
        return _dts
    except: return ''
