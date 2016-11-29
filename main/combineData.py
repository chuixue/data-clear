#coding:utf8
'''
Created on 11.28, 2016

@author: Administrator
'''
from pymongo import MongoClient
import datetime
import re
import public as P
import config as CFG
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def combineCompanyInfo(cfg, newName, oldName):
    print 'read new company information by name'
    lsQuali = {}
    lsHonor = {}
    lsCourt = {}
    lsBadbe = {}
    lsOpera = {}
    id = 0
    oldRecord = {}
    for item in cfg.writeCompany.find({'company_name': newName}):
        id = item['id']
        for q in item['qualification']:
            lmd5 = "".join([q['type'], q['class'], q['professional'], q['level']])
            lsQuali[lmd5] = q
        for q in item['honors']:
            lmd5 = "".join([q['sourcesUrl'], q['honorName'], q['sources'], q['awardDate']])
            lsHonor[lmd5] = q
        for q in item['courtRecords']:
            lmd5 = "".join([q['caseName'], q['courtName'], q['caseCode'], q['sources']])
            lsCourt[lmd5] = q
        for q in item['badbehaviors']['badBehaviorDetails']:
            lmd5 = "".join([q['behaviorFact'], q['punishUnit'], q['releaseTime']])
            lsBadbe[lmd5] = q
        for q in item['operationDetail']:
            lmd5 = "".join([q['notifiedDate'], q['messageName'], q['sources']])
            lsOpera[lmd5] = q
    print 'qualification:{}，honor:{}，court:{}，badbehavior:{}, operation:{}'.format(len(lsQuali), len(lsHonor), len(lsCourt), len(lsBadbe), len(lsOpera))
    
    print 'read old company information by name'
    for item in cfg.writeCompany.find({'company_name': oldName}):
        oldRecord = item
        del oldRecord["_id"]
        for q in item['qualification']:
            lmd5 = "".join([q['type'], q['class'], q['professional'], q['level']])
            if lmd5 not in lsQuali: lsQuali[lmd5] = q
        for q in item['honors']:
            lmd5 = "".join([q['sourcesUrl'], q['honorName'], q['sources'], q['awardDate']])
            if lmd5 not in lsHonor: lsHonor[lmd5] = q
        for q in item['courtRecords']:
            lmd5 = "".join([q['caseName'], q['courtName'], q['caseCode'], q['sources']])
            if lmd5 not in lsCourt: lsCourt[lmd5] = q
        for q in item['badbehaviors']['badBehaviorDetails']:
            lmd5 = "".join([q['behaviorFact'], q['punishUnit'], q['releaseTime']])
            if lmd5 not in lsBadbe: lsBadbe[lmd5] = q
        for q in item['operationDetail']:
            lmd5 = "".join([q['notifiedDate'], q['messageName'], q['sources']])
            if lmd5 not in lsOpera: lsOpera[lmd5] = q
    print 'qualification:{}，honor:{}，court:{}，badbehavior:{}, operation:{}'.format(len(lsQuali), len(lsHonor), len(lsCourt), len(lsBadbe), len(lsOpera))
    
    print 'update table companyInfo'
    obj = {'qualification':lsQuali.values(), 'honors':lsHonor.values(), 'courtRecords':lsCourt.values(), 'badbehaviors.badBehaviorDetails':lsBadbe.values(), 'operationDetail':lsOpera.values()}
    obj['updateTime'] = datetime.datetime.now()
    
    if id!=0:
        cfg.writeCompany.update({'company_name':newName}, {'$set':obj})
    else:
        oldRecord['company_name'] = newName
        oldRecord['id'] = P.getMaxId(cfg.companyInfo, 'id') + 1
        for key in obj: oldRecord[key] = obj[key]
        if 'badbehaviors' not in oldRecord: oldRecord['badbehaviors'] = {'creditScore':100, 'badBehaviorDetails':[]}
        if 'badBehaviorDetails' not in oldRecord['badbehaviors']: oldRecord['badbehaviors']['badbehaviors'] = []
        oldRecord['badbehaviors']['badBehaviorDetails'] = obj['badbehaviors.badBehaviorDetails']
        del oldRecord['badbehaviors.badBehaviorDetails']
        cfg.writeCompany.insert(oldRecord)
    return True

def combineBidding(cfg, newName, oldName):
    print 'read new bidding information by name'
    lsbiddi = {}
    lsData = []
    id = 0
    for item in cfg.writeBidding.find({'company_name': newName, 'type':'中标'}):
        id = item['company_id']
        lmd5 = "".join([item['architects'], item['biddingPrice'], item['biddingDate'], item['projectName']])
        lsbiddi[lmd5] = item
    print 'new company records count:', len(lsbiddi)
    
    if id==0:
        lsComp = P.getCompanyId(cfg.writeCompany)
        if newName in lsComp: id = lsComp[newName.encode('utf8')] 
    if id==0: return False
    
    for item in cfg.writeBidding.find({'company_name': oldName, 'type':'中标'}):
        lmd5 = "".join([item['architects'], item['biddingPrice'], item['biddingDate'], item['projectName']])
        del item["_id"]
        item['id'] = id
        item['company_name'] = newName
        if lmd5 not in lsbiddi: lsData.append(item)
    print 'insert records count:', len(lsData)
    cfg.writeBidding.insert(lsData)

def combineData(cfg, newName, oldName):
    combineCompanyInfo(cfg, newName, oldName)
    combineBidding(cfg, newName, oldName)
    
if __name__ == '__main__':
    print 'Hello Moto..'
    dt = datetime.datetime.now()
    #*********************************************
    _cfg = CFG.Config()
    combineData(_cfg, "四川蜀望生态环保科技有限公司2", "四川蜀望生态环保科技有限公司")

#    combineCompanyInfo(_cfg, "四川蜀望生态环保科技有限公司", "四川蜀望建设工程有限公司")
#    combineBidding(_cfg, "四川蜀望生态环保科技有限公司", "四川蜀望建设工程有限公司")
    

    
    #*********************************************
    print datetime.datetime.now(), datetime.datetime.now()-dt
    print 'End !'