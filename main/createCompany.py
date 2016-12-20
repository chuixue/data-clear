#coding:utf8
'''
Created on Sep 14, 2016

@author: Administrator
'''
from pymongo import MongoClient
import datetime
import re
import public as P
import libCompany as libC
import config as CFG
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


#汇总入川企业资质
def _readCompanyOut(cfg, cursor):
    print 'read company information out...'
    lsComp = {}
    
    index = 0
    for item in cursor:
        if 'companyName' not in item: continue
        cp = item['companyName']
        ctype = re.sub('入川', '', item['companyBases'][0]['enterpriseType'].encode('utf8'))
        lines = libC.getLinesOut(item)
        
        if cp not in lsComp:
            line = libC.initLine(item)
            line['company_type'] = '入川'
            line['companyBases']['enterpriseType'] = { ctype:1 }
            lsComp[cp] = line
        for line in lines:
            '''----------------------新旧专业映射--------------------------------------'''
            if line[0]!='工程施工': continue
            _tp = libC.cProfessionals
            if line[2] in _tp: line[2] = _tp[line[2]]
            '''''''''''''''''''''''''''''''''''''''''''''''----------------------------'''
            lmd5 = ','.join(line)
            lsComp[cp]['qualification'][lmd5] = { 'type':line[0], 'class':line[1], 'professional':line[2], 
                                                  'level':line[3], 'code':line[4], 'validityDates':line[5] }
            lsComp[cp]['companyBases']['enterpriseType'][ctype] = 1
    
    print 'read OK!'
    return lsComp

def getCompanyOriginal(cfg):
    return dict((item['company_name'], item) for item in cfg.companyInfo.find())
    
def getQualification(qua):
    return dict((','.join([q['type'], q['class'], q['professional'], q['level'], q['code'], q['validityDates']]), q) for q in qua)

def writeCompanyOut(cfg, lsComp):
    print 'select the max id.'
    index = P.getMaxId(cfg.companyInfo, 'id') + 1
    lsOrig = getCompanyOriginal(cfg) 
    lsInsert = []
    lsUpdate = []
    for comp in lsComp:
        if comp in lsOrig:
            lsComp[comp]['qualification'] = dict(getQualification(lsOrig[comp]['qualification']), **lsComp[comp]['qualification']).values()
            lsComp[comp]['company_qualification'] = ','.join([v['professional']+v['level'] for v in lsComp[comp]['qualification'].values()])
            lsUpdate.append([{'id':lsOrig[comp]['id']}, {'$set':{'qualification':lsComp[comp]['qualification'],
                                 'company_qualification':lsComp[comp]['company_qualification']}}])
            print comp
            continue
        lsComp[comp]['companyBases']['enterpriseType'] = lsComp[comp]['companyBases']['enterpriseType'].keys()
        lsComp[comp]['company_qualification'] = ','.join([v['professional']+v['level'] for v in lsComp[comp]['qualification'].values()])
        lsComp[comp]['qualification'] = lsComp[comp]['qualification'].values()
        lsComp[comp]['id'] = index
        index += 1
        lsInsert.append(lsComp[comp])
    print 'write into table', len(lsInsert), '...'
    #cfg.writeCompany.insert(lsInsert)
    
    print 'update some company', len(lsUpdate), '...'
    for d in lsUpdate:
        cfg.writeCompany.update(d[0], d[1])
    print 'complete!'


def readCompanyIn(cfg):
    cursor = cfg.tbProvenceIn.find({})
    return _readCompanyIn(cfg, cursor)

def readCompanyOut(cfg):
    cursor = cfg.tbProvenceOut.find({})
    return _readCompanyOut(cfg, cursor)
    
def _readCompanyIn(cfg, cursor):
    print 'read company information in...'
    lsComp = {}
    for item in cursor:
        if 'companyName' not in item: continue
        cp = item['companyName']
        ctype = item['companyBases'][0]['enterpriseType'].encode('utf8')
        if ctype=='建设单位' or ctype=='': continue
        lines = libC.getLines(item)
        
        if cp not in lsComp:
            line = libC.initLine(item)
            line['company_type'] = '省内'
            line['companyBases']['enterpriseType'] = { ctype:1 }
            lsComp[cp] = line
        for line in lines:
            '''--------------------------专业映射--------------------------------------'''
            _tp = libC.cProfessionalsIn
            if line[2] in _tp: line[2] = _tp[line[2]]
            '''''''''''''''''''''''''''''''''''''''''''''''----------------------------'''
            lmd5 = ','.join(line)
            lsComp[cp]['qualification'][lmd5] = { 'type':line[0], 'class':line[1], 'professional':line[2], 
                                                  'level':line[3], 'code':line[4], 'validityDates':line[5] }
            lsComp[cp]['companyBases']['enterpriseType'][ctype] = 1
    print 'read OK!'
    return lsComp

def writeCompanyIn(cfg, lsComp):
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
    cfg.writeCompany.insert(dt)
    
    
if __name__ == '__main__':
    print 'Hello '
    dt = datetime.datetime.now()
    #*********************************************
    _cfg = CFG.Config()
    readCompanyIn(_cfg)
#    
#    exit()
#    readCompanyOut(_cfg)
    
    
    #writeCompanyIn(_cfg, readCompanyIn(_cfg))
    #writeCompanyOut(_cfg, readCompanyOut(_cfg))
    
    _cfg.writeCompany.create_index('id')

    
    #*********************************************
    print datetime.datetime.now(), datetime.datetime.now()-dt
    print 'End !'