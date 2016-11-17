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
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def cout(ls):
    for l in ls: print l, ':', ls[l],
    print
def out(ls):
    for l in ls: print l,
    print

#汇总入川企业资质
def readCompanyOut(cfg):
    print 'read company information out...'
    lsComp = {}
    for item in cfg.tbProvenceOut.find():
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
    
def writeCompanyOut(cfg, lsComp):
    print 'select the max id.'
    index = P.getMaxId(cfg.companyInfo, 'id') + 1
    dt = []
    for comp in lsComp:
        lsComp[comp]['companyBases']['enterpriseType'] = lsComp[comp]['companyBases']['enterpriseType'].keys()
        lsComp[comp]['company_qualification'] = ','.join([v['professional']+v['level'] for v in lsComp[comp]['qualification'].values()])
        lsComp[comp]['qualification'] = lsComp[comp]['qualification'].values()
        lsComp[comp]['id'] = index
        index += 1
        dt.append(lsComp[comp])
    print 'write into table', len(lsComp), '...'
    cfg.write.insert(dt)   
    print 'complete!'
    
def readCompanyIn(cfg):
    print 'read company information in...'
    lsComp = {}
    for item in cfg.tbProvenceIn.find():
        if 'companyName' not in item: continue
        cp = item['companyName']
        ctype = item['companyBases'][0]['enterpriseType'].encode('utf8')
        if ctype=='建设单位': continue
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
    cfg.write.insert(dt)

def addIdForSpecialCompany(cfg):
    index = 0
    lsUpdate = []
    lsCompany = P.getCompanyId(cfg.dbNow.companyInfoNew)
    
    for item in cfg.dbNow.SpecialCondition.find():
        cpname = item['company_name'].encode('utf8')
        id = lsCompany[cpname] if cpname in lsCompany else 0 
        lsUpdate.append([{'company_name': item['company_name']}, {'$set':{'company_id':id}}])
    print 'update all the data,', len(lsUpdate)
    for b in lsUpdate: 
        cfg.dbNow.SpecialCondition.update(b[0], b[1])
        index += 1
        if index % 2000 == 0: print index
    print 'OK!' 
    
    
if __name__ == '__main__':
    print 'Hello '
    dt = datetime.datetime.now()
    #*********************************************
    _cfg = CFG.Config()
    
    writeCompanyIn(_cfg, readCompanyIn(_cfg))
    writeCompanyOut(_cfg, readCompanyOut(_cfg))
        
    _cfg.write.create_index('id')
    
    addIdForSpecialCompany(_cfg)
    
    #*********************************************
    print datetime.datetime.now(), datetime.datetime.now()-dt
    print 'End !'