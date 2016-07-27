#coding:gb18030
'''
Created on Jul 24, 2016

@author: Administrator
'''
from pymongo import MongoClient
import re


def cout(ls):
    for l in ls: print l, ':', ls[l],
    print
    
def deal():
    onnection = MongoClient('192.168.3.45', 27017)
    db = onnection['Ent_Person']
    provence = db.EInProvenceDetail
    
    index = 0
    ls = {}
    for item in provence.find():    
        if item['companyName'] not in ls: ls[item['companyName']] = 1
        else:
            index += 1
            print item['companyName']
    print index
    
def main():
    con = MongoClient('localhost', 27017)
    db = con['middle']    
    write = db.companyInfo4
    company_table = db.companyInfo2
    
    index = 0
    id = 10000000
    ls = {}
    lines = []
    for item in company_table.find():
        line = item
        line['id'] = str(id)
        id += 1 
        lines.append(line)
#        if item['company_name'] not in ls: ls[item['company_name']] = 1
#        else: 
##            print item['company_name']
#            index +=1
    
    write.insert(lines)
    print index

def personAddId():
    con = MongoClient('localhost', 27017)
    db = con['middle']    
    write = db.person4
    person_table = db.person3
    
    index = 0
    id = 50000000
    ls = {}
    lines = []
    for item in person_table.find():
        line = item
        line['id'] = str(id)
        id += 1 
        lines.append(line)
#        if item['company_name'] not in ls: ls[item['company_name']] = 1
#        else: 
##            print item['company_name']
#            index +=1
    
    write.insert(lines)
    print index

def calcCompany():
    con = MongoClient('localhost', 27017)
    db = con['middle']    
    company = db.companyInfo4
    
    ls = {}
    for item in company.find():
        if 'qualificationType' not in item: continue
        
        for v in item['qualificationType']:
            if v['name'] == '设计与施工一体化':
                for p in v['professionalType']:
                    if p['name'] not in ls: ls[p['name']] = 1
                    else:
                        ls[p['name']] += 1
#                print v['professionalType']

    l = 0
    for l in ls:
        l += ls[1]
        print l,':',ls[l]
#    cout(ls)
#    print(ls.keys())
    print l

#def calcPerson():
#    con = MongoClient('localhost', 27017)
#    db = con['middle']    
#    company = db.person4
#    
#    ls = {}
#    tp = Type()
#    for item in company.find():
#        if 'certificate' not in item: return
#        line = []
#        for c in item['certificate']:
#            key = tp.getType(c)
#            if not key: continue
#            keys = ['jz', 'ak', 'zj', 'zjy', 'aq']
#            if key == 'jz':
#                lname = '注册建造师'
#                lprofessional = 
#                llevel = 
#                lcode = 
#                lvalidityDate = 
#                
#                cout(c)
#                
##            if p['name'] not in ls: ls[p['name']] = 1
#        
#
#    l = 0
#    for l in ls:
#        l += ls[1]
#        print l,':',ls[l]
#
##    cout(ls)
##    print(ls.keys())
#    print l

class Type(object):
    def getType(self, _item):
        if self.IS_JianZaoShi(_item):return 'jz'
        elif self.IS_AnKaoZheng(_item):return 'ak'
        elif self.IS_ZaoJiaGongChengShi(_item):return 'zj'
        elif self.IS_ZaoJiaYuan(_item):return 'zjy'        
        elif self.IS_ZhuCeAnQuan(_item):return 'aq' 
        
    def IS_JianZaoShi(self,item):   #注册建造师
        if "type" not in item: return False
        str = item["type"]
        if(str.find('建造')==-1):
            return False;
        else:
            return True;
    
    def IS_AnKaoZheng(self, item):  #安考证
        if "code" in item: 
            str = item["code"]
            if (str.find('建') >=0 and str.find('安') >=0 ): return True;
            if (str.find('水') >=0 and str.find('安') >=0 ): return True;
        return False;
    
    def IS_ZaoJiaGongChengShi(self, item):  #造价师
        if "code" in item:
            if (item["code"].find('造') >= 0): return True; 
        else:
            if 'staffLevel' in item and 'type' in item and item['type'].find("造价工程师")>=0 and item['staffLevel'].find("造价工程师")>=0: return True
        return False;
    
    def IS_ZaoJiaYuan(self, item):  #
        if 'code' in item and 'type' in item:
            if (item["code"].find('造') == -1 and item["type"].find("造价工程师")>=0): return True;
        else:
            if 'staffLevel' in item and item["type"].find("造价工程师")>=0 and item['staffLevel'].find("造价员")>=0: return True
        return False
        
    def IS_ZhuCeAnQuan(self, item):
        if 'registeredtype' in item and item['registeredtype'].find('安全')>=0: return True
        return False
        
  
def addCompanyBase():
    connection = MongoClient('192.168.3.45', 27017)
    con2 = MongoClient('171.221.173.154', 27017)
    con = MongoClient('localhost', 27017)
    db = connection['Ent_Person']
    db1 = con['middle']
    db2 = connection['constructionDB']
    db3 = con2['jianzhu']
    
    companyInfo = db1.companyInfo5
    write = db1.companyInfo3
    
    ls = []
    lsInfo = {}
    lsGood = {}
    lsUpdate = []
    lsCourt = {} 
    lsHonor = {}
    lsBidding = {}
    print 'Hello'
    for item in db2.companyAchievement.find():
        lsBidding[item['companyName']] = item['biddingDetail']
        if len(item['honors'])>0: lsHonor[item['companyName']] = item['honors']
    
    for item in db2.courtRecord.find():
        if 'companyName' not in item: continue
        cpname = item['companyName']
        if cpname not in lsCourt: lsCourt[cpname] = []
        else: lsCourt[cpname].append(item)
    
    for item in db2.goodBehavior.find():
        if item['goodBehavior'] != []: lsGood[item['companyName']] = item['goodBehavior']

    for item in db2.badBehavior.find():
        lsInfo[item['companyName']] = [item['creditScore'], item['badBehaviorDetail']] 
    
    for item in companyInfo.find():
        cpname = item['company_name']
        item['badbehaviors'] = {"creditScore": 100, "badBehaviorDetails": [] }
        item['courtRecords'] = []
        item['honors'] = []
        item['bidding'] = []
        if cpname in lsInfo: 
            item['badbehaviors'] = {"creditScore": lsInfo[cpname][0], "badBehaviorDetails": lsInfo[cpname][1] }
        if cpname in lsGood:
            item['goodbehaviors'] = lsGood[cpname]
        if cpname in lsCourt:
            item['courtRecords'] = lsCourt[cpname]
        if cpname in lsHonor:
            item['honors'] = lsHonor[cpname]
        if cpname in lsBidding:
            ls.append(cpname)
            item['bidding'] = lsBidding[cpname]
        
        lsUpdate.append(item)
    for cp in ls:
        print cp
#    write.insert(lsUpdate)
    
    return
#    for item in db.EInProvenceDetail.find():
#        lsInfo[item['entId']] = item['companyBases'][0]
#        lg = item['companyBases'][0]['legalRepresentative']
#        if lg not in lsInfo: lsInfo[lg] = 1
#        print item['companyBases'][0]['organizationCode']
#        code = item['companyBases'][0]['organizationCode']
#        if not haveNum(code): print code, '____', item['companyBases'][0]['legalRepresentative']
#        cpname = item['company_name']
#        if not haveNum(code): print code, '____', item['companyBases'][0]['legalRepresentative']
        
        
#        print code

#        st.add(code)   organizationCode   legalRepresentative         
    
    cout(lsInfo)
#    for s in st: print s
    
    return
    index = 0
    lsAll = []
    print len(lsInfo)
    
    
    for item in companyInfo.find():
        if item['companyBases'] == {}: continue
        print item['companyBases']['legalRepresentative']
        
#        if 'company_id' in item and item['company_id'] in lsInfo:
#            index += 1
#            item['companyBases'] = lsInfo[item['company_id']]
#        else:
#            item['companyBases'] = []
#        else:
#            print item
#            print item['company_id']
#        if item['company_name'].strip() != '' : lsAll.append(item)
        
#    write.insert(lsAll)
    print index, companyInfo.find().count()


def updateCompany():
    connection = MongoClient('192.168.3.45', 27017)
    con2 = MongoClient('171.221.173.154', 27017)
    con = MongoClient('localhost', 27017)
    db = connection['Ent_Person']
    db1 = con['middle']
    db2 = connection['constructionDB']
    db3 = con2['jianzhu']
    
    companyInfo = db1.companyInfo5
#    write = con2['jianzhu3'].companyInfo
    write = db1.companyInfo
    
    ls = []
    lsGood = {}
    lsUpdate = []
    lsCourt = {} 
    lsHonor = {}
    lsBidding = {}
    for item in db2.companyAchievement2.find():
        lsBidding[item['companyName']] = item['biddingDetail']
        if len(item['honors'])>0: lsHonor[item['companyName']] = item['honors']
    index = 0
    
    for item in companyInfo.find():
        cp = item['company_name'].strip()
        if len(cp)<4:
            index += 1
            ls.append(cp)
            print cp
    
    print write.remove({'company_name': {"$in": ls}})
#        print 
    
    print index
    return
    for item in companyInfo.find({"company_name":{"$in":lsBidding.keys()}}):
        item['honors'] = lsHonor[item['company_name']] 
        item['bidding'] = lsBidding[item['company_name']]
#        print item['company_name']
#        write.update({'company_name':item['company_name']},item)
    
    
def haveNum(_s):
    st = set(_s)
    num = dict([[str(i), 1] for i in range(0,10)])
    for s in st:
        if s in num: return True
    return False

if __name__ == '__main__':
#    main()
#    deal()
#    personAddId()
#    calcCompany()
#    calcPerson()
#    addCompanyBase()
    updateCompany()
#    print haveNum('qwiqnbwui')
    
    
    