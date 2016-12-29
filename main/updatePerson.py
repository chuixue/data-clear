#coding:utf8
'''
Created on Sep 29, 2016

@author: Administrator
'''
#from pymongo import MongoClient
import datetime
import re
import public as P
import createPerson as CP
import config as CFG
import libLog as LG
import libPerson as LP

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def getPersonOriginal(cfg):
    '''return : { personId:{id:n, certificate:{md5:}} }'''
    lsPsIds = {}    #正规personId
    lsOther = {}    #其它personId
    lsCpNms = {}    #company_name + name索引
    for item in cfg.updatePerson.find():
        cp = item['company_name'].encode('utf8')
        pId = item['personId']
        key = (cp + '_'+ item['name']).encode('utf8')
        if key not in lsCpNms: lsCpNms[key] = {}
        _tp = {'id':item['id'], 'certificate':dict((','.join([c['name'], c['professional'], c['level'], c['code'], c['validityDate']])
                                                                 ,1) for c in item['certificate'])} 
        if LP.isPersonId(pId):
            lsPsIds[pId] = _tp 
            lsCpNms[key][pId] = lsPsIds[pId]
        else:
            lsOther[pId] = _tp
            lsCpNms[key][pId] = lsOther[pId]
    return lsPsIds, lsOther, lsCpNms
    
def sortString(str):
    return ','.join(sorted(str.split(',')))

def md5ToLine(md5):
    c = md5.split(',')
    return {'name':c[0], 'professional':c[1], 'level':c[2], 'code':c[3], 'validityDate':c[4]}

class readPersonData(object):
    def __init__(self, cfg):
        self.cfg = cfg
        self.db = self.cfg.dbPerson
        self.RP = CP.ReadPerson(cfg)
        self.callback = {'personnelInPCopy':self.RP._read_personnelInPCopy, 'personnelEnterPCopy':self.RP._read_personnelEnterPCopy,
                         'WCSafetyEngineer':self.RP._read_WCSafetyEngineer, 'safetyEngineer':self.RP._read_safetyEngineer,
                         'CERegistered':self.RP._read_CERegistered, 'WCEngineer':self.RP._read_WCEngineer
                         }
    '''可根据需要设置数据过滤条件'''
    def readPersonDataFilter(self, tb):
        cursor = self.db[tb].find({})#.limit(50000)
        return self.callback[tb](cursor)

'''''''''''''''''''''''''''''''''
    ↓↓最烧脑过程↓↓
'''''''''''''''''''''''''''     
'''
    lsCpNms只保存引用，缓存用
'''
def checkNewRecord(cfg, newPersonDic):
    lg = LG.Log()
    lg.log('select the max id.')
    index = P.getMaxId(cfg.updatePerson, 'id') + 1
    if index==0: index = 50000001
    lsPsIds, lsOther, lsCpNms = getPersonOriginal(cfg)
    lsNew = {}
    '''有personId数据'''
    print len(newPersonDic)
    print 'original records count', len(lsPsIds), len(lsOther), len(lsCpNms)
    for p in newPersonDic:
        if not LP.isPersonId(p): continue
        cp = newPersonDic[p]['company_name'].strip()
        if p in lsPsIds:
            for c in newPersonDic[p]['certificate']:
                if c in lsPsIds[p]['certificate']: continue
                lsPsIds[p]['certificate'][c] = 1
                lsNew[p] = 1
        else:
            key = (cp + '_'+ newPersonDic[p]['name']).encode('utf8')
            _tp = {'id':index,'data':newPersonDic[p], 'certificate':newPersonDic[p]['certificate']} 
            index += 1
            lsPsIds[p] = _tp
            if key not in lsCpNms: lsCpNms[key] = {}
            lsCpNms[key][p] = lsPsIds[p]
            lsNew[p] = 1
            
    '''可合并数据·无正规personId'''
    for p in newPersonDic:
        if LP.isPersonId(p): continue
        cp = newPersonDic[p]['company_name'].strip()
        _tp = {'id':index,'data':newPersonDic[p], 'certificate':newPersonDic[p]['certificate']}
        if cp!='':
            key = (cp + '_'+ newPersonDic[p]['name']).encode('utf8')
            if key in lsCpNms:
                if p in lsCpNms[key]:   #personId已记录 
                    for c in newPersonDic[p]['certificate']:
                        if c in lsCpNms[key][p]['certificate']: continue
                        lsCpNms[key][p]['certificate'][c] = 1  
                        lsNew[p] = 1
                else:
                    index += 1
                    lsOther[p] = _tp
                    lsCpNms[key][p] = lsOther[p]
                    lsNew[p] = 1
            else:   
                index += 1
                lsOther[p] = _tp
                if key not in lsCpNms: lsCpNms[key] = {} 
                lsCpNms[key][p] = lsOther[p]
                lsNew[p] = 1
        else:
            if p in lsOther:
                for c in newPersonDic[p]['certificate']:
                    if c in lsOther[p]['certificate']: continue
                    lsOther[p]['certificate'][c] = 1  
                    lsNew[p] = 1
                    print 4
            else:
                index += 1
                lsOther[p] = _tp
                lsNew[p] = 1
                print 5
        '''End If cp'''
    '''End for p'''
    
    #-----------------------------------------------------------------------------------
    print len(lsPsIds), len(lsOther), len(lsCpNms)
    return [lsPsIds, lsOther, lsNew] 
        
def readPerson(cfg): 
    lg = LG.Log()
    rp = readPersonData(cfg)
    tables = ['personnelInPCopy', 'personnelEnterPCopy', 'WCSafetyEngineer', 'safetyEngineer', 'CERegistered', 'WCEngineer']
    for tb in tables:
        '''依次处理各表'''
        lg.log( 'read and deal table：', tb)
        rp.readPersonDataFilter(tb)
        
    '''合并数据'''
    personDic = CP.combinePersonNoIdByName(rp.RP.personDic)
    ''''''
    data = checkNewRecord(cfg, personDic)
    return data


'''插入数据准备'''
def formatInsert(lp, p, lsCompany):
    cpname = lp['data']['company_name']
    lp['data']['id'] = lp['id']   
    lp['data']['personId'] = p
    lp['data']['certificate'] = [md5ToLine(c) for c in lp['certificate']]
    lp['data']['label'] = 0
    lp['data']['other'] = ""
    lp['data']['updateTime'] = datetime.datetime.now()
    lp['data']['companyname'] = cpname
    lp['data']['company_id'] = lsCompany[cpname] if cpname in lsCompany else 0
     
'''更新数据准备'''
def formatUpdate(lp):
    rst = {'updateTime': datetime.datetime.now(), 'label':0}
    rst['certificate'] = [md5ToLine(c) for c in lp['certificate']]
    lp['temp'] = [{'id':lp['id']}, {'$set':rst}]
            
def updatePerson(cfg, dataset):
    lsCompany = P.getCompanyId(cfg.companyInfo)
    lg = LG.Log()
    lsPsIds = dataset[0]  
    lsOther = dataset[1]  
    lsNew = dataset[2]
    lsInsert = []
    lsUpdate = []
    
    for dts in [lsPsIds, lsOther]:
        for p in dts:
            if p not in lsNew: continue
            if 'data' in dts[p]:
                formatInsert(dts[p], p, lsCompany)
                lsInsert.append(dts[p]['data'])
            else:
                formatUpdate(dts[p])
                lsUpdate.append(dts[p]['temp'])
        '''End For p'''
    '''End For dts'''
        
    lg.log('update table ', len(lsUpdate), ' ...')
    for d in lsUpdate: cfg.updatePerson.update(d[0], d[1])
    lg.log('insert into table', len(lsInsert), ' ...')
    if len(lsInsert)>0: cfg.updatePerson.insert(lsInsert)
    lg.log('complete!')
    lg.save()
    
        
    
if __name__ == '__main__':
    dt = datetime.datetime.now()
    #*********************************************
    _cfg = CFG.Config()
    

    updatePerson(_cfg, readPerson(_cfg))
        
#    checkNewRecord(_cfg)

    
    
    #*********************************************
    print datetime.datetime.now(), datetime.datetime.now()-dt
    print 'End !'