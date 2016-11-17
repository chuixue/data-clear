#coding:gb18030
'''
Created on Jul 24, 2016

@author: Administrator
'''
from pymongo import MongoClient
import re
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


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
    db3 = con2['jianzhu3']
    
    companyInfo = db1.companyInfo
#    write = db1.companyInfo3
    write = db3.companyInfo
    
    ls = []
    lsInfo = {}
    lsGood = {}
    lsUpdate = []
    lsCourt = {} 
    lsHonor = {}
    lsBidding = {}
    print 'Hello'
    for item in db2.companyAchievement2.find():
        lsBidding[item['companyName']] = item['biddingDetail']
        if len(item['honors'])>0: lsHonor[item['companyName']] = item['honors']
    
    for item in db2.courtRecord.find({'companyName':'四川佳和建设工程有限公司'.encode('utf8')}):
        if 'companyName' not in item: continue
        cpname = item['companyName']
        if cpname not in lsCourt: lsCourt[cpname] = [item]
        else: lsCourt[cpname].append(item)
    
#    it = {}
#    for item in companyInfo.find({'company_name':'四川佳和建设工程有限公司'.encode('utf8')}):
#        print item['company_name']
#        it = item
#    it['courtRecords'] = lsCourt[cpname] 
#    
#    write.update({'company_name':'四川佳和建设工程有限公司'.encode('utf8')}, item)
#    return

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

def addCompanyCourt():
    connection = MongoClient('192.168.3.45', 27017)
    con2 = MongoClient('171.221.173.154', 27017)
    con = MongoClient('localhost', 27017)
    db = connection['Ent_Person']
    db1 = con['middle']
    db2 = connection['constructionDB']
    db3 = con2['jianzhu']
    
    index = 0
    companyInfo = db1.companyInfo5
    
#    write = con2['jianzhu3'].companyInfo
    write = con2['jianzhu3'].companyInfo#_copy
#    write = db1.companyInfo
#    print db3.person5.find({'certificate.name':'注册建造师'.encode('utf8')}).count()
#    return
    
    lsCourt = {}
    for item in db2.courtRecord.find():
        cpname = ''
#        if 'companyName' not in item: continue
#        cpname = item['companyName']
        if 'companyName' in item: cpname = item['companyName'] 
        else: cpname = item['pname']
        if cpname not in lsCourt: lsCourt[cpname] = [item]
        else: lsCourt[cpname].append(item)
    print len(lsCourt)
    count = 6057
    pdic = {}
    for item in companyInfo.find():
        cp = item['company_name']
        if cp not in lsCourt: continue
        item['courtRecords'] = lsCourt[cp] 
        write.update({'company_name':cp}, item)
        index += 1
        pid = int(1.0 * index / count * 100)
        if pid % 10 == 0 and pid not in pdic: 
            pdic[pid] = 1
            print cp
            print str(pid) + '%' 

    
#def updateCompany():
#    connection = MongoClient('192.168.3.45', 27017)
#    con2 = MongoClient('171.221.173.154', 27017)
#    con = MongoClient('localhost', 27017)
#    db = connection['Ent_Person']
#    db1 = con['middle']
#    db2 = connection['constructionDB']
#    db3 = con2['jianzhu']
#    
#    companyInfo = db1.companyInfo5
##    write = con2['jianzhu3'].companyInfo
#    write = db1.companyInfo
#    
#    ls = []
#    lsInfo = {}
#    lsGood = {}
#    lsUpdate = []
#    lsCourt = {} 
#    lsHonor = {}
#    lsBidding = {}
#    index = 0
#    
#    #去除名字非法
#    for item in companyInfo.find(): 
#        cp = item['company_name']
#        if len(item['company_name'])<4: print cp
#        
#        if item['company_name'].find('')>=0: print cp
#        
##        if len(item['company_name'])<4: ls.append(item['company_name'])
#        
##    print write.remove({'company_name': {"$in": ls}})
#    return

#    cpio = con2['jianzhu3'].companyInfo.find()
#    for item in cpio:
#        cp = item['company_name']
#        if cp not in lsInfo: lsInfo[cp] = 1
#    print cpio.count(), len(lsInfo)
    
#    return
#    lsId = {}
#    for item in con['middle'].companyInfo.find(): lsId[item['id']] = item['company_name']
#    for item in con2['jianzhu3'].companyInfo.find():
#        print lsId[item['id']], item['company_name']
#        if lsId[item['id']] != item['company_name']: print item['company_name']
#    return
    
    #**************************************************************************
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

    #**************************************************************************  
#    #businessLicense 与 creditCode对调
#    for item in companyInfo.find():
#        cpname = item["company_name"]
#        if type(item['badbehaviors']) == type([]): item['badbehaviors'] = item['badbehaviors'][0] #修正
#        item['companyBases']['creditCode'] = ''
#        if 'businessLicense' in item['companyBases']:
#            item['companyBases']['creditCode'] = item['companyBases']['businessLicense']
#        item['companyBases']['businessLicense'] = ''
#        item['bidding'] = []
#        item['honors'] = []
#        if cpname in lsInfo:
#            for key in lsInfo[cpname]['companyBases']:  item['companyBases'][key] = lsInfo[cpname]['companyBases'][key]
#            item['bidding'] = lsInfo[cpname]['bidding']
#            item['honors'] = lsInfo[cpname]['honors']
#            item['companyBases']['businessLicense'] = lsInfo[cpname]['businessLicense']
#            item['companyBases']['profile'] = lsInfo[cpname]['profile']
#        ls.append(item)
#    write.insert(ls)
    
    #**************************************************************************
#    for item in companyInfo.find({"company_name":{"$in":lsInfo.keys()}}):
#        cpname = item["company_name"]
#        for key in lsInfo[cpname]['companyBases']: 
#            item['companyBases'][key] = lsInfo[cpname]['companyBases'][key]
#        item['bidding'] = lsInfo[cpname]['bidding']
#        item['honors'] = lsInfo[cpname]['honors']
#        write.update({'company_name': cpname }, item)
#        print cpname

#填补人员表里公司信息-根据公司表里人员personId
#def addPersonCompany():
#    con2 = MongoClient('171.221.173.154', 27017)
#    con = MongoClient('localhost', 27017)
#    db1 = con['middle']
#    db3 = con2['jianzhu']
#    
#    write = db1.person1
#    companyInfo = db1.companyInfo
#    person = db1.person
#    index = 0
#    
#    lsPerson = {}
#    for item in companyInfo.find():
#        cp = item['company_name']
#        for line in item['certificate']:
#            if line['personID'] not in lsPerson: lsPerson[line['personID']] = {cp:1} 
#            else: 
#                if cp not in lsPerson[line['personID']]: lsPerson[line['personID']][cp] = 1 
#    lines = []
#    for item in person.find():
#        pid = item['personId']
#        if pid not in lsPerson: continue
#        ls = {}
#        flg = False
#        for cp in item['companyname']:
#            if cp.strip() == "": 
#                flg = True
#                continue
#            ls[cp] = 1
#        for cp in lsPerson[pid]:
#            if cp in ls: continue
#            flg = True
#            ls[cp] = 1
#        if flg: #需要更新
#            index += 1
#            if index % 1000 == 0: print '第',index,'条'
#            item['companyname'] = ls.keys()
#            lines.append([pid, ls.keys()])  #表更新方式
##        lines.append(item)    #重建表方式
#
##    write.insert(lines)    #重建表方式
#    for line in lines: write.update({'personId':line[0]},{'$set':{'companyname':line[1]}})
#    print 'last record：',pid
#    print '共更新',index,'条记录. person1'

#更新中标和荣誉信息
def updateBidding():
    connection = MongoClient('192.168.3.45', 27017)
    con2 = MongoClient('171.221.173.154', 27017)
    con = MongoClient('localhost', 27017)
    db = connection['Ent_Person']
    db1 = con['middle']
    db2 = connection['constructionDB']
    db3 = con2['jianzhu3']
    
    companyInfo = db1.companyInfo
#    write = db1.companyInfo
    write = db3.companyInfo
    
#    print db1.companyInfo_0805.find({'honors':[]}).count()
#    return 
    index = 0
    lsHonor = {}
    lsBidding = {}
    for item in db2.companyAchievement2.find():
        lsBidding[item['companyName']] = item['biddingDetail']
        lsHonor[item['companyName']] = item['honors']
    
    for item in db1.companyInfo_0805.find({"company_name":{"$in":lsBidding.keys()}}):
        cpname = item["company_name"]
        if cpname not in lsBidding: continue
        if len(item['bidding']) == len(lsBidding[cpname]) and len(item['honors']) == len(lsHonor[cpname]): continue 
        item['bidding'] = lsBidding[cpname]
        item['honors'] = lsHonor[cpname]
        index += 1
        if index % 500 == 0: print '第',index,'条'
#        write.update({'company_name': cpname }, {'$set':{'bidding':item['bidding'], 'honors':item['honors']}})
        print cpname
    print '共更新公司总数：',index
 
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
#    updateCompany()
#    addCompanyCourt()
#    addPersonCompany()
    
#    updateBidding()
#    updateCompany()
#    print haveNum('qwiqnbwui')
    pass
    
    