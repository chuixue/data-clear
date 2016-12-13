#coding:utf8
'''
Created on Sep 29, 2016

@author: Administrator
'''
#from pymongo import MongoClient
import datetime
import datetime
import re
import public as P
import createCompany as CC
import config as CFG


import sys
reload(sys)
sys.setdefaultencoding('utf-8')



def getCompanyOriginal(cfg):
    return dict((item['company_name'], [item['company_qualification'], item['id']]) for item in cfg.midCompany.find())
     

def updateCompanyIn(cfg):
    print 'select the max id.'
    index = P.getMaxId(cfg.companyInfo, 'id') + 1
    if index==0: index = 10000001
    lsOrig = getCompanyOriginal(cfg)    
    lsComp = CC._readCompanyIn(cfg, cfg.tbProvenceIn.find({}))
    
    lsInsert = []
    lsUpdate = []
    
    print len(lsOrig)

    for comp in lsComp:
        cpql = ','.join([v['professional']+v['level'] for v in lsComp[comp]['qualification'].values()])         
        lsComp[comp]['companyBases']['enterpriseType'] = lsComp[comp]['companyBases']['enterpriseType'].keys()
        lsComp[comp]['company_qualification'] = cpql
        lsComp[comp]['qualification'] = lsComp[comp]['qualification'].values()
        lsComp[comp]['updateTime'] = datetime.datetime.now() 
        if comp in lsOrig or comp.encode('utf8') in lsOrig:
            if lsOrig[comp][0]==cpql: 
                continue
            else:
                print comp
                print '\t',cpql
                print '\t',lsOrig[comp][0]
                lsUpdate.append([{'id':lsOrig[comp][1]}, {'$set':lsComp[comp]}])
        else:
            print comp
            lsComp[comp]['id'] = index
            index += 1
            lsInsert.append(lsComp[comp])
            
    for n in lsInsert:
        pass
    
    print 'insert into table', len(lsInsert), '...'
    #cfg.writeCompany.insert(dt)
    print 'complete!'
    
    
    
    
    
if __name__ == '__main__':
    dt = datetime.datetime.now()
    #*********************************************
    _cfg = CFG.Config()
    
    #getCompanyOriginal(_cfg)
    updateCompanyIn(_cfg)


    
    #*********************************************
    print datetime.datetime.now(), datetime.datetime.now()-dt
    print 'End !'