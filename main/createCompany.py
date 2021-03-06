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
import libLog as LG
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

def writeCompanyOut(cfg, lsComp, dealStyle = 0):
    lg = LG.Log()
    lg.log('select the max id.')
    writer = cfg.writeCompany if dealStyle==0 else cfg.updateCompany 
    index = P.getMaxId(cfg.companyInfo, 'id') + 1
    lsOrig = getCompanyOriginal(cfg) 
    lsInsert = []
    lsUpdate = []
    for comp in lsComp:
        if comp in lsOrig:
            lsComp[comp]['qualification'] = dict(getQualification(lsOrig[comp]['qualification']), **lsComp[comp]['qualification']).values()
            lsComp[comp]['company_qualification'] = ','.join([v['professional']+v['level'] for v in lsComp[comp]['qualification']])
            lsUpdate.append([{'id':lsOrig[comp]['id']}, {'$set':{'qualification':lsComp[comp]['qualification'],
                                 'company_qualification':lsComp[comp]['company_qualification']}}])
            continue
        lsComp[comp]['companyBases']['enterpriseType'] = lsComp[comp]['companyBases']['enterpriseType'].keys()
        lsComp[comp]['company_qualification'] = ','.join([v['professional']+v['level'] for v in lsComp[comp]['qualification'].values()])
        lsComp[comp]['qualification'] = lsComp[comp]['qualification'].values()
        lsComp[comp]['id'] = index
        lsComp[comp]['label'] = 0
        index += 1
        lsInsert.append(lsComp[comp])
    lg.log( 'write into table', len(lsInsert), '...')
    if len(lsInsert)>0: writer.insert(lsInsert)
    lg.log( 'update some repeat company', len(lsUpdate), '...')
    for d in lsUpdate: writer.update(d[0], d[1])
    lg.log( 'complete!')
    lg.save()

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
    lg = LG.Log()
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
    lg.log( 'write', len(lsComp), 'records')
    cfg.writeCompany.insert(dt)
    lg.save()
    
    
if __name__ == '__main__':
    print 'Hello '
    dt = datetime.datetime.now()
    #*********************************************
    _cfg = CFG.Config()
#    readCompanyIn(_cfg)    
#    readCompanyOut(_cfg)
    
    
    writeCompanyIn(_cfg, readCompanyIn(_cfg))
    writeCompanyOut(_cfg, readCompanyOut(_cfg))
    
    _cfg.writeCompany.create_index('id')

    
    #*********************************************
    print datetime.datetime.now(), datetime.datetime.now()-dt
    print 'End !'