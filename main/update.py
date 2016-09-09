#encoding:utf8
'''
Created on Sep 1, 2016

@author: Administrator
'''
import pymongo
from pymongo import MongoClient
import time
import datetime
import re

def cout(ls):
    for l in ls: print l, ':', ls[l],
    print
def out(ls):
    for l in ls: print l,
    print
def haveNum(_s):
    if (not _s) or _s.strip()=='': return False
    st = set(_s)
    num = dict([[str(i), 1] for i in range(0,10)])
    for s in st:
        if s in num: return True
    return False
def Date_F(str):
    str = str.decode('utf8')
    if not str or str == '': return ''
    if not (str.find("年")>0 and str.find("月")>0): return str.encode('utf8')
    sp = re.split("年|月".decode('utf8'), str)
    return (sp[0] + '-' + sp[1] + '-' + sp[2].replace('日', '')).encode('utf8')

#更新所有诉讼记录    线上    0:34:01.796000
def updateNewCourt():
    con1 = MongoClient('localhost', 27017)
    con2 = MongoClient('192.168.3.45', 27017)
    con3 = MongoClient('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    
    write = db3.companyInfoNew
    
    index = 0
    lsCourt = {}
    lsTemp = {}
    #*************************************************************************************
    print 'read courtRecord table...'
    for item in db2.courtRecord.find():
        cpname = item['companyName'].encode('utf8') if 'companyName' in item else item['pname'].encode('utf8')    #有人名
        if cpname not in lsCourt: lsCourt[cpname] = {}
        code = item['caseCode'].encode('utf8')
        if code in lsTemp: continue  
        lsTemp[code] = 1
        item['companyName'] = cpname
        item['caseDate'] = Date_F(item['caseCreateTime'])
        item['courtName'] = item['execCourtName']
        keys = ["caseCode", "caseDate", "courtName", "companyName", "execMoney", "companyType"]
        ktps = ["original_message", "content_url", "caseName", "caseType", "judicial_procedure"]
        line = dict((k, item[k]) for k in keys)
        for k in ktps: line[k] = ""  
        lsCourt[cpname][code] = line
        
    lsTemp = {}
    #************************************************************************************* 
    print 'read judgment table...'
    for item in db2.judgment.find({}, {'doc_content':0}):
        code = re.sub('）', ')', item['case_number'].encode('utf8'))
        code = re.sub('（', '(', code)
        cpname = item['companyName'].encode('utf8')
        if cpname not in lsCourt: lsCourt[cpname] = {}
        if code in lsTemp: continue 
        lsTemp[code] = 1
        for c in [['caseCode', 'case_number'], ['caseDate', 'judge_date'], ['caseName', 'case_name'], ['courtName', 'court_name'], ['content_url', 'doc_content_url'], ['caseType', 'case_type']]:item[c[0]] = item[c[1]]
        keys = ["caseCode", "caseDate", "companyName", "courtName", 'caseName', "original_message", "content_url", "caseType", "judicial_procedure"]
        ktps = ["execMoney", "companyType"]
        line = dict((k, item[k]) for k in keys)
        for k in ktps: line[k] = ""  
        
        if code in lsCourt[cpname]:
            line["execMoney"] = lsCourt[cpname][code]["execMoney"]   
            line["companyType"] = lsCourt[cpname][code]["companyType"]
            lsCourt[cpname][code] = line
            index += 1
            continue
        lsCourt[cpname][code] = line
    print index,'records find in judgment'
    #*************************************************************************************
    print 'read companyInfoNew table...'
    lsUpdate = []
    temp = {'company_name':1, 'id':1}
    for item in db1.companyInfoNew2.find({}).limit(1):lskey = dict((key, 0) for key in item if key not in temp)
    for item in db1.companyInfoNew2.find({}, lskey):
        if item['company_name'].encode('utf8') not in lsCourt: continue
        lsUpdate.append([{'id':item['id']}, {'$set':{'courtRecords':lsCourt[item['company_name'].encode('utf8')].values()}}])
    
    index = 0
    print 'update', len(lsUpdate), 'records...'
    for b in lsUpdate: 
        write.update(b[0], b[1])
        index += 1
        if index % 5000 == 0: print '\t',index
    print index, 'complete!'
    

#更新公司诉讼记录    -    0:01:58.866000s
def updateCourt():
    con1 = MongoClient('localhost', 27017)
    con2 = MongoClient('192.168.3.45', 27017)
    con3 = MongoClient('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    
    write = db1.companyInfoNew2
    
    index = 0
    lsCourt = {}
    lsTemp = {}
    for item in db2.courtRecord.find():
        cpname = item['companyName'] if 'companyName' in item else item['pname']    #有人名
        if cpname not in lsCourt: lsCourt[cpname] = []
        if item['caseCode'].encode('utf8') in lsTemp: continue #未测试 
        lsTemp[item['caseCode'].encode('utf8')] = 1
        lsCourt[cpname].append(item)
    
    for item in db1.companyInfoNew.find():
        obj = {}
        cp = item['company_name']
        if cp in lsCourt: obj['courtRecords'] = lsCourt[cp]
        if len(obj)==0: continue
        obj['updateTime'] = datetime.datetime.now()
        write.update({'company_name':cp}, {'$set':obj })
        if index % 1000 ==0: 
            print index,
            print cp
        index += 1
    print '源诉讼公司数：', len(lsCourt)
    print '更新公司数：', index

#更新中标、荣誉、操作三字段信息    -    0:06:20.632000
def updateBidding():
    con1 = MongoClient('localhost', 27017)
    con2 = MongoClient('192.168.3.45', 27017)
    con3 = MongoClient('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    
    write = db1.companyInfoNew2
     
    index = 0
    lsHonor = {}
    lsBidding = {}
    lsoperation = {}
    for item in db2.companyAchievement.find():
        if len(item['biddingDetail'])>0: 
            lsBidding[item['companyName']] = []
            for line in item['biddingDetail']:
                if line['projectName'] == '': continue
                if line['projectName'][-4:] == "[反馈]": line['projectName'] = line['projectName'][0:-4]
                lsBidding[item['companyName']].append(line)
        if len(item['honors'])>0: lsHonor[item['companyName']] = item['honors']
        lsoperation[item['companyName']] = item['operationDetail']
    for item in db1.companyInfoNew.find():
        obj = {}
        cpname = item["company_name"]
        if cpname in lsBidding: obj['bidding'] = lsBidding[cpname] 
        if cpname in lsHonor: obj['honors'] = lsHonor[cpname]
        if cpname in lsoperation: obj['operationDetail'] = lsoperation[cpname]
        if len(obj)==0: continue
        obj['updateTime'] = datetime.datetime.now()
        write.update({'company_name': cpname }, {'$set':obj })
        if index % 5000 == 0: 
            print index,
            print cpname
        index += 1
    print '共更新公司总数：',index
    
#更新优良、不良记录    -    0:00:20.538000
def updateGoodRecord():
    con1 = MongoClient('localhost', 27017)
    con2 = MongoClient('192.168.3.45', 27017)
    con3 = MongoClient('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    
    write = db1.companyInfoNew2
    
    index = 1
    lsBad = {}
    lsGood = {}
    for item in db2.goodBehavior.find():
        if len(item['goodBehavior'])>0: lsGood[item['companyName']] = item['goodBehavior']
    for item in db2.badBehavior.find():
        lsBad[item['companyName']] = [item['creditScore'], item['badBehaviorDetail']] 
        
    for item in db1.companyInfoNew.find():
        obj = {}
        cpname = item['company_name']
        if cpname in lsBad: 
            obj['badbehaviors'] = {"creditScore": lsBad[cpname][0], "badBehaviorDetails": lsBad[cpname][1] }
        if cpname in lsGood: obj['goodbehaviors'] = lsGood[cpname]
        if len(obj)==0: continue
        obj['updateTime'] = datetime.datetime.now()
        write.update({'company_name': cpname }, {'$set':obj })
        if index % 500 ==0: 
            print index,
            print cpname
        index += 1
    print '共更新公司总数：',index

#给公司表添加公司其它基本信息
def updateCompanyBase():
    con1 = MongoClient('localhost', 27017)
    con2 = MongoClient('192.168.3.45', 27017)
    con3 = MongoClient('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    
    write = db3.companyInfoNew
    
    lsAch = {}
    lsUpdate = []
    temp = {'company_name':1, 'id':1, 'companyBases':1}
    for item in db3.companyInfoNew.find({}).limit(1):lskey = dict((key, 0) for key in item if key not in temp)
    for item in db2.companyAchievement.find():
        line = dict((k, '') for k in ['contactPhone', 'fax', 'address', 'postcode' ])
        for key in line: line[key] = item['companyContact'][0][key] 
        line['profile'] = item['companyProfile'][0]['profile'] if len(item['companyProfile'])>0 else ''
        line['profile'] = line['profile'].encode('utf8')[16:].strip() 
        lsAch[item['companyName'].encode('utf8')] = line
        
    print 'update all the companyInfoNew...'
    for item in db3.companyInfoNew.find({}, lskey):
        cpname = item['company_name'].encode('utf8')
        if cpname not in lsAch:
            st = dict(('companyBases.'+k, '') for k in ['contactPhone', 'fax', 'address', 'postcode', 'profile'])
            st['profile'] = '暂无信息'
            lsUpdate.append([{'id':item['id']}, {'$set':st}])
            continue
        line = lsAch[cpname]
        line['organizationCode'] = item['companyBases']['organizationCode'] if 'organizationCode' in item['companyBases'] else ''
        line['legalRepresentative'] = item['companyBases']['legalRepresentative'] if 'legalRepresentative' in item['companyBases'] else ''
        ccode = line['organizationCode']
        if not haveNum(ccode):
            line['organizationCode'] = line['legalRepresentative'] 
            line['legalRepresentative'] = ccode
        st = dict(('companyBases.'+k, line[k]) for k in line)
        lsUpdate.append([{'id':item['id']}, {'$set':st}])
    for d in lsUpdate:
        write.update(d[0], d[1])
    print 'complete', len(lsUpdate), 'records.'
    
    
#更新所有附加信息    -    0:06:23.216000
def updateCompanyOther():   
    con1 = MongoClient('localhost', 27017)
    con2 = MongoClient('192.168.3.45', 27017)
    con3 = MongoClient('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    
    write = db1.companyInfoNew3
    
    index = 0
    lsBad = {}
    lsGood = {}
    lsCourt = {} 
    lsHonor = {}
    lsBidding = {}
    lsoperation = {}
    lsUpdate = []
    for item in db2.courtRecord.find():
        cpname = item['companyName'] if 'companyName' in item else item['pname']    #有人名
        if cpname not in lsCourt: lsCourt[cpname] = []
        lsCourt[cpname].append(item)
    for item in db2.goodBehavior.find():
        if item['goodBehavior'] != []: lsGood[item['companyName']] = item['goodBehavior']
    for item in db2.badBehavior.find():
        lsBad[item['companyName']] = [item['creditScore'], item['badBehaviorDetail']] 
    for item in db2.companyAchievement.find():
        if len(item['biddingDetail'])>0: 
            lsBidding[item['companyName']] = []
            for line in item['biddingDetail']:
                if line['projectName'] == '': continue
                if line['projectName'][-4:] == "[反馈]": line['projectName'] = line['projectName'][0:-4]
                lsBidding[item['companyName']].append(line)
        if len(item['honors'])>0: lsHonor[item['companyName']] = item['honors']
        lsoperation[item['companyName']] = item['operationDetail']
    
    for item in db1.companyInfoNew.find():
        obj = {}
        cpname = item['company_name']
        if cpname in lsBad: 
            obj['badbehaviors'] = {"creditScore": lsBad[cpname][0], "badBehaviorDetails": lsBad[cpname][1] }
        if cpname in lsGood: obj['goodbehaviors'] = lsGood[cpname]
        if cpname in lsCourt: obj['courtRecords'] = lsCourt[cpname]
        if cpname in lsHonor: obj['honors'] = lsHonor[cpname]
        if cpname in lsBidding: obj['bidding'] = lsBidding[cpname]
        if len(obj)==0: continue
        obj['updateTime'] = datetime.datetime.now()
        write.update({'company_name': cpname }, {'$set':obj })
        if index % 5000 ==0: 
            print index,
            print cpname
        index += 1
        
def clearInfo():
    con1 = MongoClient('localhost', 27017)
    con2 = MongoClient('192.168.3.45', 27017)
    con3 = MongoClient('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    
    write = db1.companyInfoNew2
    
    index = 0
    for item in db1.companyInfoNew.find():
        cpname = item['company_name']
        write.update({'company_name':cpname},{'$set':{'honors':[], 'bidding':[], 'goodbehaviors':[], 
                                'operationDetail':[], 'courtRecords':[], 'operationDetail':[]}})
        if index % 5000 == 0: print index
        index += 1
        
if __name__ == '__main__':
    dt = datetime.datetime.now()
#    clearInfo()
#    updateCourt()
#    updateBidding()
#    updateGoodRecord()
#    updateCompanyOther()
#    updateCompanyBase()
#    updateNewCourt()
    updateCompanyBase()
    
    
    print datetime.datetime.now(), datetime.datetime.now()-dt