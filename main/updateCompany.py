#coding:utf8
'''
Created on Sep 29, 2016

@author: Administrator
'''
#from pymongo import MongoClient
import datetime
import re
import public as P
import createCompany as CC
import config as CFG
import libLog as LG

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def getCompanyOriginal(cfg):
    return dict((item['company_name'], [item['company_qualification'], item['id']]) for item in cfg.midCompany.find())
    
def sortString(str):
    return ','.join(sorted(str.split(',')))

'''引用createCompany模块writeCompanyOut()直接处理原始数据并更新公司表'''  
def updateCompanyOut(cfg):
    CC.writeCompanyOut(_cfg, readCompanyOut(_cfg))
    
'''可重写，添加查询时间过滤'''
def readCompanyOut(cfg):
    cursor = cfg.tbProvenceOut.find({})
    return CC._readCompanyOut(cfg, cursor)
    
'''可重写，添加查询时间过滤'''    
def updateCompanyIn(cfg):
    cursor = cfg.tbProvenceIn.find({})
    return _updateCompanyIn(cfg, cursor)
    

'''引用createCompany模块_readCompanyIn()处理原始数据'''    
def _updateCompanyIn(cfg, cursor):
    lg = LG.Log()
    lg.log('select the max id.')
    index = P.getMaxId(cfg.companyInfo, 'id') + 1
    if index==0: index = 10000001
    lsOrig = getCompanyOriginal(cfg)    
    lsComp = CC._readCompanyIn(cfg, cursor)
    lg.log('origin records count ', len(lsOrig))
    
    lsInsert = []
    lsUpdate = []
    for comp in lsComp:
        cpql = ','.join([v['professional']+v['level'] for v in lsComp[comp]['qualification'].values()])         
        lsComp[comp]['companyBases']['enterpriseType'] = lsComp[comp]['companyBases']['enterpriseType'].keys()
        lsComp[comp]['company_qualification'] = cpql
        lsComp[comp]['qualification'] = lsComp[comp]['qualification'].values()
        lsComp[comp]['updateTime'] = datetime.datetime.now() 
        if comp in lsOrig or comp.encode('utf8') in lsOrig:
            if sortString(lsOrig[comp][0])==sortString(cpql):
                continue
            else:
                lsUpdate.append([{'id':lsOrig[comp][1]}, {'$set':lsComp[comp]}])
        else:
            lg.log('new company: ' + comp, False)
            lsComp[comp]['id'] = index
            index += 1
            lsInsert.append(lsComp[comp])
    lg.log('update table ', len(lsUpdate), ' ...')
    for d in lsUpdate: cfg.writeCompany.update(d[0], d[1])
    lg.log('insert into table', len(lsInsert), ' ...')
    if len(lsInsert)>0: cfg.writeCompany.insert(lsInsert)
    lg.log('complete!')
    lg.save()
    
    
if __name__ == '__main__':
    dt = datetime.datetime.now()
    #*********************************************
    _cfg = CFG.Config()
    
    #getCompanyOriginal(_cfg)
    
    updateCompanyIn(_cfg)
    updateCompanyOut(_cfg)

    
    #*********************************************
    print datetime.datetime.now(), datetime.datetime.now()-dt
    print 'End !'