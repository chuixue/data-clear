#coding:utf8
'''
Created on Sep 5, 2016
解析文书网
@author: Administrator
'''
import pymongo
from pymongo import MongoClient
import time
import datetime
import re
import public as P

def cout(ls):
    for l in ls: print l, ':', ls[l],
    print
def out(ls):
    for l in ls: print l,
    print

#将companyAchievement表业绩统一整理到业绩表   
def addCompanyBidding():
    con1 = MongoClient('localhost', 27017)
    con2 = MongoClient('192.168.3.45', 27017)
    con3 = MongoClient('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    write = db3.bidding
    
    lsComp = {}
    lsTemp = {}
    lsUpdate = []
    lskey = P.dbKeys(db1.bidding, ['id'])
    index = db1.bidding.find({}, lskey).sort('id', -1).limit(1)[0]['id'] + 1
    print 'select company list for id.'
    lsCpId = P.getCompanyId(db3.companyInfoNew)
    print 'select useful company.'
    for item in db2.companyAchievement.find({}, P.dbKeys(db2.companyAchievement, ['companyName'])): 
        lsComp[item['companyName'].encode('utf8')] = 1
    print 'read existing bidding of company.'
    for item in db1.bidding.find({'type':'中标'}):
        cpname = item['company_name'].encode('utf8')
        pj = item['projectName'].encode('utf8')
        if cpname=='' or cpname not in lsComp: continue
        lsTemp[cpname+'_'+pj] = 1
    print 'read new bidding of company.'
    cpl = ['sources', 'sourcesUrl', 'biddingDate', 'biddingPrice', 'projectName', 'architects']
    for item in db2.companyAchievement.find({'biddingDetail':{'$gt':[]}}):
        cpname = item['companyName'].encode('utf8')
        for b in item['biddingDetail']:
            pj = b['projectName'][:-4].encode('utf8')
            if cpname+'_'+pj in lsTemp: continue
            line = dict((k, b[k]) for k in cpl)
            if line['biddingPrice'].encode('utf8').find('暂无信息')!=-1: line['biddingPrice'] = ''
            line['updateTime'] = datetime.datetime.now()
            line['biddingDate'] = line['biddingDate'].strip() 
            line['type'] = '中标'
            line['content'] = ''
            line['announcementId'] = ''
            line['company_name'] = cpname
            line['label'] = 0
            line['company_id'] = 0 if cpname not in lsCpId else lsCpId[cpname]
            line['id'] = index
            index += 1 
            lsUpdate.append(line)
    print 'insert new records,',len(lsUpdate), '...'
    write.insert(lsUpdate)
    print 'complete!'
    
#已弃用
def addBiddingCompanyId():
    con1 = MongoClient('localhost', 27017)
    con2 = MongoClient('192.168.3.45', 27017)
    con3 = MongoClient('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    write = db1.bidding
    
    index = 0
    lskey = {}
    lsCompany = {}
    lsUpdate = []
    for item in db3.companyInfoNew.find({}).limit(1):lskey = dict((key, 0) for key in item if key!='company_name' and key !='id') 
    for item in db3.companyInfoNew.find({}, lskey): lsCompany[item['company_name'].encode('utf8')] = item['id']
    print len(lsCompany)
    for item in db1.bidding.find():#({'company_name':{'$gt':''}}).limit(2):
        cpname = item['company_name'].encode('utf8')
        id = lsCompany[cpname] if cpname in lsCompany else -1 
        lsUpdate.append([{'id': item['id']}, {'$set':{'company_id':id, 'label':0}}])
    print 'update all the data,', len(lsUpdate)
    for b in lsUpdate: 
        write.update(b[0], b[1])
        index += 1
        if index % 5000 == 0: print index
    print 'OK!'
        
def deal():
    con1 = MongoClient('localhost', 27017)
    con2 = MongoClient('192.168.3.45', 27017)
    con3 = MongoClient('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    
    for item in db2.tb.find({}, {}):
        print item
    print db1.companyInfoNew2.find().count()    
     #读取较详细数据
#    for item in db2.companyAchievement2.find():
#        cpname = item['companyName'] 
#        tp = { 'contactPhone':'', 'fax':'', 'address':'', 'postcode':'' }
#        lsInfo[cpname] = { 'bidding':[], 'honors':[], 'companyBases':tp }
#        lsInfo[cpname]['bidding'] = item['biddingDetail']
#        lsInfo[cpname]['profile'] = item['companyProfile'][0]['profile']
#        lsInfo[cpname]['businessLicense'] = item['companyProfile'][0]['businessLicense']
#        if len(item['honors'])>0: lsInfo[cpname]['honors'] = item['honors']
#        for key in tp: lsInfo[cpname]['companyBases'][key] = item['companyContact'][0][key] 
def selectCompanyPersonCount():
    con1 = MongoClient('localhost', 27017)
    db1 = con1['middle']
#    write = db1.temp
#    lsCP = {}
#    ls = []
#    for item in db1.personNew.find():
#        cp = item['companyname'].encode('utf8')
#        if cp not in lsCP: lsCP[cp] = set()
#        lsCP[cp].add(item['name'].encode('utf8'))
#    for cp in lsCP:
#        ls.append({'company_name':cp, 'count':len(lsCP[cp])})
#    write.insert(ls)
    
    for item in db1.temp.find().sort('count', -1).limit(100):
        print item['company_name'], item['count']

def temp():
    con1 = MongoClient('localhost', 27017)
    con3 = MongoClient('171.221.173.154', 27017)
    db1 = con1['middle']
    db3 = con3['jianzhu3']
    temp = {'company_name':1, 'id':1}
    
    write = db3.SpecialCondition
    
    lsCompany = {}
    lsUpdate = []
    index = 0
    for item in db1.companyInfoNew.find({}).limit(1):lskey = dict((key, 0) for key in item if key not in temp)
    for item in db1.companyInfoNew.find({}, lskey): lsCompany[item['company_name'].encode('utf8')] = item['id']
    
    for item in db3.SpecialCondition.find():
        cpname = item['company_name'].encode('utf8')
        id = lsCompany[cpname] if cpname in lsCompany else 0 
        lsUpdate.append([{'company_name': item['company_name']}, {'$set':{'company_id':id}}])
    print 'update all the data,', len(lsUpdate)
    for b in lsUpdate: 
        write.update(b[0], b[1])
        index += 1
        if index % 5000 == 0: print index
    print 'OK!'
    
if __name__ == '__main__':
    dt = datetime.datetime.now()
#    addBiddingCompanyId()
#    temp()
#    selectCompanyPersonCount()
    addCompanyBidding()
    
    
    

    #*********************************************
    print datetime.datetime.now(), datetime.datetime.now()-dt
    