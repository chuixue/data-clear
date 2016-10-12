#coding:utf8
'''
Created on Aug 12, 2016

@author: Administrator
'''
import pymongo
import datetime
import re
#from pymongo import ASCENDING, DESCENDING
from pymongo import MongoClient

def cout(ls):
    for l in ls: print l, ':', ls[l],
    print
def out(ls):
    for l in ls: print l,
    print
    
def clearBing():
    con1 = pymongo.Connection('localhost', 27017)
    con2 = pymongo.Connection('192.168.3.45', 27017)
    con3 = pymongo.Connection('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    
    write = db1.companyInfo
    index = 0
    for item in db1.companyInfo.find():
        cpname = item['company_name']
        write.update({'company_name':cpname},{'$set':{'honors':[], 'bidding':[], 'operationDetail':[]}})
        index += 1
        if index % 1000 == 0: print index
#        print item['honors'], item['bidding']
        
#更新中标和荣誉信息
def updateBidding():
    con1 = pymongo.Connection('localhost', 27017)
    con2 = pymongo.Connection('192.168.3.45', 27017)
    con3 = pymongo.Connection('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    
    write = db3.companyInfo
    
    count = 0
    index = 0
    lsHonor = {}
    lsBidding = {}
    lsoperation = {}
    for item in db2.companyAchievement.find():
#        lsBidding[item['companyName']] = item['biddingDetail']
        lsHonor[item['companyName']] = item['honors']
        lsoperation[item['companyName']] = item['operationDetail']
        lsb = []
        for line in item['biddingDetail']:
            if line['projectName'] == '': continue
            if line['projectName'][-4:] == "[反馈]": line['projectName'] = line['projectName'][0:-4]
            lsb.append(line)
        lsBidding[item['companyName']] = lsb
    for item in db1.companyInfo.find():
        cpname = item["company_name"]
        if cpname not in lsBidding: continue
#        if len(item['bidding']) == len(lsBidding[cpname]) and len(item['honors']) == len(lsHonor[cpname]): continue 
        item['bidding'] = lsBidding[cpname]
        item['honors'] = lsHonor[cpname]
        item['operationDetail'] = lsoperation[cpname]
        if len(item['bidding'])!=0:count+=1
        write.update({'company_name': cpname }, {'$set':{'bidding':item['bidding'], 'honors':item['honors'],'operationDetail':item['operationDetail']}})
        index += 1
        if index % 1000 == 0: print index
        
    print '共更新公司总数：',index
    print '中标信息',count
    
#去掉末尾反馈字样
def removeSome():
    con1 = pymongo.Connection('localhost', 27017)
    con2 = pymongo.Connection('192.168.3.45', 27017)
    con3 = pymongo.Connection('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    
    index = 0
    write = db1.companyInfo
    for item in db1.companyInfo.find():
        if item['bidding']==[]: continue
        ls = []
        for line in item['bidding']:
            if line['projectName'][-4:] == "[反馈]":
                line['projectName'] = line['projectName'][0:-4]
            ls.append(line)
        write.update({'company_name': item['company_name'] }, {'$set':{'bidding':ls}})
        index += 1
        if index % 1000 == 0:
            print index
            print item['company_name']
    print '共更新：',index
    
#去掉建设单位性质公司
def removeSomeCompany():
    con1 = pymongo.Connection('localhost', 27017)
    con2 = pymongo.Connection('192.168.3.45', 27017)
    con3 = pymongo.Connection('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    
    write = db3.companyInfo
    index = 0
    lsp = {}
    for item in db2.enterprises.find():
        cpname = item['companyName']
        type = item['type'].strip()
        if cpname not in lsp: lsp[cpname] = {type:1}
        else: lsp[cpname][type] = 1
    lsremove = {}
    for p in lsp:
        if len(lsp[p])==1 and lsp[p].keys()[0]=='': lsremove[p] = 1
    for item in db1.companyInfo3.find():
        cpname = item['company_name']
#        if 'enterpriseType' in item['companyBases']: print item['companyBases']['enterpriseType']
        if cpname in lsremove:
            if cpname.find("公司")==-1:
                write.remove({'company_name': cpname })
                index+=1
    print index
    
def dealDesign():
    con1 = pymongo.Connection('localhost', 27017)
    con2 = pymongo.Connection('192.168.3.45', 27017)
    con3 = pymongo.Connection('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    
    write = db1.companyInfo3
#    # 清字段
#    ls = []
#    for item in db1.companyInfo.find():
#        item['qualificationType'] = []
#        item['company_qualification'] = []
#        ls.append(item)
#    write.insert(ls)
    
    lst = set()
    for item in db2.EInProvenceDetail.find():
        tp = item['companyBases'][0]['enterpriseType']
        lst.add(tp)
        
    for l in lst:
        print l 
#    print lst


def selectTemp():
    con1 = MongoClient('localhost', 27017)
    con2 = MongoClient('192.168.3.45', 27017)
    con3 = MongoClient('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    
    write = db1.companyInfo3
    st = set()
    for item in db2.WCSafetyEngineer.find():
        code = item['certificateCode']
        cp = re.split('\('.decode('utf8'), item['certificateCode'])
#        print cp[0]
        st.add(cp[0][0:-1])
#        print cp[0][-1:]
#        print code
#        print code.find('\(')
    for s in st:
        print s
 
if __name__ == '__main__':
    dt = datetime.datetime.now()
    
#    clearBing()    
#    updateBidding()
#    removeSome()
#    removeSomeCompany()
    
#    dealDesign()
#    selectTemp()
    
    print datetime.datetime.now(), datetime.datetime.now()-dt
    

    
    print ''
    
    
    pass


