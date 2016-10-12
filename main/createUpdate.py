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
import config as CFG

def cout(ls):
    for l in ls: print l, ':', ls[l],
    print
def out(ls):
    for l in ls: print l,
    print

def Date_F(str):
    str = str.decode('utf8')
    if not str or str == '': return ''
    if not (str.find("年")>0 and str.find("月")>0): return str.encode('utf8')
    sp = re.split("年|月".decode('utf8'), str)
    return (sp[0] + '-' + sp[1] + '-' + sp[2].replace('日', '')).encode('utf8')



class ReadCourt(object):
    def __init__(self, cfg):
        self.lsCourt = {}
        self.cfg = cfg
        self.db = self.cfg.dbSource
    def log(self): print '--', len(self.lsCourt), 'Records collect.'

    def read_courtRecord(self):
        lsTemp = {}
        print 'read courtRecord table...'
        for item in self.db.courtRecord.find():
            cpname = item['companyName'].encode('utf8') if 'companyName' in item else item['pname'].encode('utf8')    #有人名
            if cpname not in self.lsCourt: self.lsCourt[cpname] = {}
            code = item['caseCode'].encode('utf8')
            if code in lsTemp: continue  
            lsTemp[code] = 1
            item['companyName'] = cpname
            item['caseDate'] = Date_F(item['caseCreateTime'])
            item['courtName'] = item['execCourtName']
            keys = ["caseCode", "caseDate", "courtName", "companyName", "execMoney", "companyType", "id"]
            ktps = ["original_message", "content_url", "caseName", "caseType", "judicial_procedure", "publish_date"]
            line = dict((k, item[k]) for k in keys)
            line['sources'] = '全国法院执行网' 
            for k in ktps: line[k] = ""  
            self.lsCourt[cpname][code] = line
        self.log()
    def read_judgment(self):
        lsTemp = {}
        index = 0 
        print 'read judgment table...'
        for item in self.db.judgment.find({}, {'doc_content':0, 'original_message':0}):
            code = re.sub('）', ')', item['case_number'].encode('utf8'))
            code = re.sub('（', '(', code)
            cpname = item['companyName'].encode('utf8')
            if cpname not in self.lsCourt: self.lsCourt[cpname] = {}
            if code in lsTemp: continue 
            lsTemp[code] = 1
            if 'publish_date' not in item: item['publish_date'] = ""
            for c in [['caseCode', 'case_number'], ['caseDate', 'judge_date'], ['caseName', 'case_name'], ['courtName', 'court_name'], ['content_url', 'doc_content_url'], ['caseType', 'case_type']]:item[c[0]] = item[c[1]]
            keys = ["caseCode", "caseDate", "publish_date", "companyName", "courtName", 'caseName', "content_url", "caseType", "judicial_procedure"]
            ktps = ["execMoney", "companyType", 'id']
            line = dict((k, item[k]) for k in keys)
            line['sources'] = '中国裁判文书网'
            for k in ktps: line[k] = ""  
            '''---------------------------------去掉臃肿字段'''
            line['original_message'] = ""   
            '''---------------------------------------------'''
            
            if code in self.lsCourt[cpname]:
                line["execMoney"] = self.lsCourt[cpname][code]["execMoney"]   
                line["companyType"] = self.lsCourt[cpname][code]["companyType"]
                self.lsCourt[cpname][code] = line
                index += 1
                continue
            self.lsCourt[cpname][code] = line
        print index,'records find in judgment'
def updateCourt(cfg, lsCourt):
    print 'read companyInfoNew table...'
    lsUpdate = []
    lsCpId = P.getCompanyId(cfg.companyInfo)
    for cp in lsCourt:
        st = {'courtRecords':lsCourt[cp].values(), 'updateTime':datetime.datetime.now()}
        if cp in lsCpId: lsUpdate.append([{'id':lsCpId[cp]}, {'$set':st}])
    index = 0
    print 'update', len(lsUpdate), 'records...'
    for b in lsUpdate: 
        cfg.writeCompany.update(b[0], b[1])
        index += 1
        if index % 5000 == 0: print '\t',index
    print index, 'complete!'
    
    
#更新所有诉讼记录    线上    0:34:01.796000    线下    0:03:20.164000
def updateNewCourt(cfg):
    rc = ReadCourt(cfg)
    rc.read_courtRecord()
    rc.read_judgment()
    print 'update court information..'
    updateCourt(cfg, rc.lsCourt)
    
#更新荣誉、操作二字段信息    -    0:06:20.632000
def updateHonors(cfg):
    index = 0
    lsHonor = {}
    lsoperation = {}
    lsUpdate = []
    for item in cfg.companyAchievement.find():
        if len(item['honors'])>0: lsHonor[item['companyName']] = item['honors']
        lsoperation[item['companyName']] = item['operationDetail']
    for item in cfg.companyInfo.find():
        obj = {}
        cpname = item["company_name"]
        if cpname in lsHonor: obj['honors'] = lsHonor[cpname]
        if cpname in lsoperation: obj['operationDetail'] = lsoperation[cpname]
        if len(obj)==0: continue
        obj['updateTime'] = datetime.datetime.now()
        lsUpdate.append([{'id':item['id']}, {'$set':obj}])
        
    print 'update', len(lsUpdate), 'records...'
    for b in lsUpdate: 
        cfg.writeCompany.update(b[0], b[1])
        index += 1
        if index % 5000 == 0: print '\t',index, b[0]
    print index, 'complete!'
    print '共更新公司总数：',index
    
    
#更新优良、不良记录    -    0:00:20.538000
def updateGoodRecord(cfg):
    index = 1
    lsBad = {}
    lsGood = {}
    lsUpdate = []
    for item in cfg.dbSource.goodBehavior.find():
        if len(item['goodBehavior'])>0: lsGood[item['companyName']] = item['goodBehavior']
    for item in cfg.dbSource.badBehavior.find():
        lsBad[item['companyName']] = [item['creditScore'], item['badBehaviorDetail'], item['detail_source'], item['detail_url']] 
    for item in cfg.companyInfo.find():
        st = {}
        cpname = item['company_name']
        if cpname in lsBad: 
            st['badbehaviors'] = {"creditScore": lsBad[cpname][0], "badBehaviorDetails": lsBad[cpname][1],
                                  'detail_source': lsBad[cpname][2], "detail_url": lsBad[cpname][3]  }
        if cpname in lsGood: st['goodbehaviors'] = lsGood[cpname]
        if len(st)==0: continue
        st['updateTime'] = datetime.datetime.now()
        lsUpdate.append([{'id':item['id']}, {'$set':st}])
    for d in lsUpdate:
        index += 1
        if index % 500 ==0: print d[0], index
        cfg.writeCompany.update(d[0], d[1])
    print '共更新公司总数：',index

#给公司表添加公司其它基本信息
def updateCompanyBase(cfg):
    index = 0
    lsAch = {}
    lsUpdate = []
    lsBase = {}
    lskey = P.dbKeys(cfg.companyInfo, ['company_name', 'id', 'companyBases'])
    print 'read the company base information...'
    for item in cfg.dbCompany.EInProvenceDetail.find({}, lskey):
        if 'companyBases' not in item: continue
        cpname = item['companyName'].encode('utf8')
        lsBase[cpname] = [item['companyBases'][0]['organizationCode'], item['companyBases'][0]['legalRepresentative']]
    for item in cfg.dbCompany.EOutProvenceDetail.find({}, lskey):
        if 'companyBases' not in item: continue
        cpname = item['companyName'].encode('utf8')
        lsBase[cpname] = [item['companyBases'][0]['organizationCode'], item['companyBases'][0]['legalRepresentative']]
    print 'read the source table...'
    for item in cfg.companyAchievement.find():
        line = dict((k, '') for k in ['contactPhone', 'fax', 'address', 'postcode' ])
        for key in line: line[key] = item['companyContact'][0][key] 
        if line['address'].find('出现错误')!=-1: line['address'] = '暂无信息' 
        line['profile'] = item['companyProfile'][0]['profile'] if len(item['companyProfile'])>0 else ''
        line['profile'] = line['profile'].encode('utf8')[16:].strip() 
        lsAch[item['companyName'].encode('utf8')] = line
    
    print 'read the companyInfoNew...'
    for item in cfg.companyInfo.find({}, lskey):
        cpname = item['company_name'].encode('utf8')
        line = dict((k, '') for k in ['contactPhone', 'fax', 'address', 'postcode', 'profile'])
        if cpname in lsAch: line = lsAch[cpname]; 
        if cpname not in lsAch: line['profile'] = '暂无信息'
        line['organizationCode'] =  lsBase[cpname][0] if cpname in lsBase else ''        
        line['legalRepresentative'] = lsBase[cpname][1] if cpname in lsBase else ''
        ccode = re.sub(str(None), '', str(line['organizationCode']).strip())
        legal = re.sub(str(None), '', str(line['legalRepresentative']).strip())
        
        _lsn1 = [P.haveNum(ccode), P.haveNum(legal), 1]; _lsn2 = [P.haveNum(ccode), P.haveNum(legal), 0]  
        line['organizationCode'] = [ccode, legal, ''][[i for i in range(3) if _lsn1[i]][0]]
        line['legalRepresentative'] = [ccode, legal, ''][[i for i in range(3) if not _lsn2[i] and _lsn2[i]!=''][0]]
        if cpname=='四川佳和建设工程有限公司':
            print 'organizationCode：', line['organizationCode'], ', legalRepresentative:', line['legalRepresentative']  
        st = dict(('companyBases.'+k, line[k]) for k in line) 
        st['updateTime'] = datetime.datetime.now()
        lsUpdate.append([{'id':item['id']}, {'$set':st}])
    print 'update all the companyInfoNew...'
    
    for d in lsUpdate:
        if index % 5000 ==0: print index, d[0]
        cfg.writeCompany.update(d[0], d[1])
        index += 1
    print 'complete', len(lsUpdate), 'records.'

        
        
if __name__ == '__main__':
    dt = datetime.datetime.now()

    _cfg = CFG.Config()
    
    updateCompanyBase(_cfg)
    updateGoodRecord(_cfg)
    updateHonors(_cfg)
    updateNewCourt(_cfg)

    print datetime.datetime.now(), datetime.datetime.now()-dt
