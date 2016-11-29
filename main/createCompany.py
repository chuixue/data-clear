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


#汇总入川企业资质
def readCompanyOut(cfg):
    print 'read company information out...'
    lsComp = {}
    
    index = 0
    for item in cfg.tbProvenceOut.find():
        if 'companyName' not in item: continue
        cp = item['companyName']
        ctype = re.sub('入川', '', item['companyBases'][0]['enterpriseType'].encode('utf8'))
        lines = libC.getLinesOut(item)
        
        index+=len(lines)
        
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
    
    print index
    
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
    cfg.writeCompany.insert(dt)
    print 'complete!'
    
def readCompanyIn(cfg):
    print 'read company information in...'
    lsComp = {}
    #cfg.tbProvenceIn.authenticate("readWriteAny","abc@123","admin")
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
    cfg.writeCompany.insert(dt)
    
    
    
if __name__ == '__main__':
    print 'Hello '
    dt = datetime.datetime.now()
    #*********************************************
    _cfg = CFG.Config()
#    readCompanyOut(_cfg)
#    
#    exit()
#    readCompanyOut(_cfg)
    
    
    writeCompanyIn(_cfg, readCompanyIn(_cfg))
    writeCompanyOut(_cfg, readCompanyOut(_cfg))
    
    _cfg.writeCompany.create_index('id')

    
    #*********************************************
    print datetime.datetime.now(), datetime.datetime.now()-dt
    print 'End !'