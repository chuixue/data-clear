#coding:utf8
'''
Created on Sep 14, 2016

@author: Administrator
'''
import pymongo
from pymongo import MongoClient
import time
import datetime
import re
import public as P
import libCompany as libC

def cout(ls):
    for l in ls: print l, ':', ls[l],
    print
def out(ls):
    for l in ls: print l,
    print
def Date_F(str):
#    str = str.decode('utf8')
    if not str or str == '': return ''
    if not (str.find("年")>0 and str.find("月")>0): return str
    sp = re.split("年|月".decode('utf8'), str)
    return (sp[0] + '-' + sp[1] + '-' + sp[2].replace('日', '')).encode('utf8')

#cBase = {'建筑业': ['专业承包', '总承包', '劳务分包', '专项资质', '施工劳务'],
#             '工程勘察': ['专业资质', '综合资质', '劳务资质'], 
#            '工程设计': ['综合资质', '行业资质', '事务所资质', '专项', '专业资质'], '工程监理': ['专业资质', '综合资质', '事务所资质'],
#            '设计施工一体化': ['设计与施工一体化'],
#            "招标代理": ["工程招标代理机构"], '造价咨询':['所有序列'], '物业服务':['所有序列'],
#            '园林绿化':['所有序列'], '房地产估价':['资质'] }
#
#cMaps = {"招标代理":"工程招标代理", "房地产估价":"房地产评估机构", "施工图审图机构":"施工图审图机构", "园林绿化":"城市园林绿化",
#        "造价咨询":"造价咨询", "物业服务":"物业服务企业", "房地产开发":"房地产开发企业", "规划编制":"城乡规划编制"}
#
#def findp(ctype, line): return [p for p in cBase[ctype] if line.find(p)!=-1]

st = set()

#汇总入川企业资质
def dealCompanyOut():
    lsComp = {}
    for item in db2.EOutProvenceDetail.find():
        if 'companyName' not in item: continue
        cp = item['companyName']
        ctype = re.sub('入川', '', item['companyBases'][0]['enterpriseType'].encode('utf8'))
        lines = libC.getLinesOut(item)
        
        if cp not in lsComp:
            line = { 'label':0, 'other':'', 'company_qualification':'', 'companyachievement':[], 
                    'badbehaviors':{"creditScore": 100, "badBehaviorDetails": [] }, 'goodbehaviors':[],  
                    'courtRecords':[], 'bidding':[], 'operationDetail':[],'courtRecords':[], 'honors':[],
                     'certificate':[], 'qualification':{},'company_name':cp, 'updateTime':datetime.datetime.now(), 
                    'company_id':item['entId'], 'companyBases':item['companyBases'][0] }
            line['company_type'] = '入川'
            line['companyBases']['enterpriseType'] = { ctype:1 }
            lsComp[cp] = line
        for line in lines:
            '''-----------------新旧专业映射--------------------------------------'''
            if line[0]!='工程施工': continue
            _tp = libC.cProfessionals
            if line[2] in _tp: line[2] = _tp[line[2]]
            '''''''''''''''''''''''''''''''''''''''''''''''----------------------------'''
            lmd5 = ','.join(line)
            lsComp[cp]['qualification'][lmd5] = { 'type':line[0], 'class':line[1], 'professional':line[2], 
                                                  'level':line[3], 'code':line[4], 'validityDates':line[5] }
            lsComp[cp]['companyBases']['enterpriseType'][ctype] = 1
    print 'select the max id.'
    index = P.getMaxId(companyInfo, 'id') + 1
    dt = []
    for comp in lsComp:
        lsComp[comp]['companyBases']['enterpriseType'] = lsComp[comp]['companyBases']['enterpriseType'].keys()
        lsComp[comp]['company_qualification'] = ','.join([v['professional']+v['level'] for v in lsComp[comp]['qualification'].values()])
        lsComp[comp]['qualification'] = lsComp[comp]['qualification'].values()
        lsComp[comp]['id'] = index
        index += 1
        dt.append(lsComp[comp])
    print 'write into table', len(lsComp), '...'
    write.insert(dt)   
    print 'complete!'
        
def dealCompany():
    con1 = MongoClient('localhost', 27017)
    con2 = MongoClient('192.168.3.45', 27017)
    con3 = MongoClient('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    lst = {}    
    
    st = set()
    ls = {}
    lsComp = {}
    for item in db2.EInProvenceDetail.find():
        if 'companyName' not in item: continue
        cp = item['companyName']
        ctype = item['companyBases'][0]['enterpriseType'].encode('utf8')
        if ctype=='建设单位': continue
        lines = libC.getLines(item)
        
        if cp not in lsComp:
            line = { 'label':0, 'other':'', 'company_qualification':'', 'companyachievement':[], 
                    'badbehaviors':{"creditScore": 100, "badBehaviorDetails": [] }, 'goodbehaviors':[],  
                    'courtRecords':[], 'bidding':[], 'operationDetail':[],'courtRecords':[], 'honors':[],
                     'certificate':[], 'qualification':{},'company_name':cp, 'updateTime':datetime.datetime.now(), 
                    'company_id':item['entId'], 'companyBases':item['companyBases'][0] }
            line['company_type'] = '省内'
            line['companyBases']['enterpriseType'] = { ctype:1 }
            lsComp[cp] = line
        for line in lines:
            lmd5 = ','.join(line)
            lsComp[cp]['qualification'][lmd5] = { 'type':line[0], 'class':line[1], 'professional':line[2], 
                                                  'level':line[3], 'code':line[4], 'validityDates':line[5] }
            lsComp[cp]['companyBases']['enterpriseType'][ctype] = 1
    
    for c in ls:
        if c not in lsComp: print c
    index = 10000001
    dt = []
    for comp in lsComp:
        lsComp[comp]['companyBases']['enterpriseType'] = lsComp[comp]['companyBases']['enterpriseType'].keys()
        lsComp[comp]['company_qualification'] = ','.join([v['professional']+v['level'] for v in lsComp[comp]['qualification'].values()])
        lsComp[comp]['qualification'] = lsComp[comp]['qualification'].values()
        lsComp[comp]['company_type'] = '省内'
        lsComp[comp]['id'] = index
        index += 1
        dt.append(lsComp[comp])
    print 'write', len(lsComp), 'records'
    write.insert(dt)

if __name__ == '__main__':
    print 'Hello '
    dt = datetime.datetime.now()
    #*********************************************
    con1 = MongoClient('localhost', 27017)
    con2 = MongoClient('192.168.3.45', 27017)
    con3 = MongoClient('171.221.173.154', 27017)
    con4 = MongoClient('192.168.3.221', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    db4 = con4['jianzhu3']
    write = db1.companyInfoNew1
    companyInfo = db1.companyInfoNew1
    
    
    dealCompany()
    dealCompanyOut()
#    select()
    
    #*********************************************
    print datetime.datetime.now(), datetime.datetime.now()-dt
    print 'End !'