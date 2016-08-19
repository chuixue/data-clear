#encoding:utf8
import pymongo
import datetime
#from pymongo import ASCENDING, DESCENDING
from pymongo import MongoClient
#from elasticsearch import Elasticsearch
import time
import datetime
import string
#import jieba
import re
import numpy as np
#from elasticsearch import Elasticsearch

import sys
#reload(sys)
#sys.setdefaultencoding( "gb18030" )
import traceback

def cout(ls):
    for l in ls: print l, ':', ls[l],
    print
def out(ls):
    for l in ls: print l,
    print


class Type(object):
    def __init__(self):self.index = 0
    
    def getType(self, _item):
        if self.IS_JianZaoShi(_item):return 'jz'
        elif self.IS_AnKaoZheng(_item):return 'ak'
        elif self.IS_ZaoJiaGongChengShi(_item):return 'zj'
        elif self.IS_ZaoJiaYuan(_item):return 'zjy'        
        elif self.IS_ZhuCeAnQuan(_item):return 'aq' 
        
    def IS_JianZaoShi(self,item):   #注册建造师
        if "type" in item and item["type"].find('建造')!=-1: return True
        return False
    
    def IS_AnKaoZheng(self, item):  #安考证
        if "certificateCode" in item: 
            str = item["certificateCode"]
            if (str.find('建') >=0 and str.find('安') >=0 ): return True;
            if (str.find('水') >=0 and str.find('安') >=0 ): return True;
        return False;
    
    def IS_ZaoJiaGongChengShi(self, item):  #造价师
        if 'personType' in item and item['personType']=='造价工程师': return True
        if "certificateCode" in item:
            if (item["certificateCode"].find('造') >= 0): return True; 
        else:
            if 'staffLevel' in item and 'type' in item and item['type'].find("造价工程师")>=0 and item['staffLevel'].find("造价工程师")>=0: return True
        return False;
    
    def IS_ZaoJiaYuan(self, item):  #
        if 'personType' in item and item['personType']=='造价员': return True
        if 'certificateCode' in item and 'type' in item:
            if (item["certificateCode"].find('造') == -1 and item["type"].find("造价工程师")>=0): return True;
        else:
            if 'staffLevel' in item and item["type"].find("造价工程师")>=0 and item['staffLevel'].find("造价员")>=0: return True
        return False
        
    def IS_ZhuCeAnQuan(self, item):
        if 'registeredType' in item and item['registeredType'].find('安全')>=0: return True
        return False


class CReadData2ES(object):

    def __init__(self):
#        self.es = Elasticsearch()
        self.Type = Type()
        pass

    def AddItem2Dict(self,item,dict,itemkey,dictkey):
        if item[itemkey]!="":
            dict[dictkey] = item[itemkey];
        return

    def typeDeal(self, type, item):
        lines = []
        if type == 'jz':
            ps = re.split(',|、'.decode('utf8'), item['professional'])
            for p in ps:
                p = p.strip().encode('utf8')
                tp = {'市政':'市政公用', '机电':'机电', '水利':'水利水电', '建筑':'建筑', '矿业':'矿业', '铁路':'铁路',
                      '公路':'公路', '港航':'港口与航道', '民航':'民航机场', '水利水电工程':'水利水电', '通信':'通信与广电'
                      ,'房屋建筑工程':'建筑', '港口':'港口与航道', '机电(限消防工程专业)':'机电'}
                if p!='':
                    if item['professional'] == '注册建造师': p = ''
                    else: p = tp[p] + '工程'
                line = ['注册建造师', p, item['staffLevel'].split('级')[0] + '级', item['certificateCode'], Date_F(item['validityDate'])]
                lines.append(line)
        elif type == 'ak':
            code = item['certificateCode']
            p = [k for k in ['水', '交', '建'] if code.find(k)>=0]
            line = ['安考证', p[0]+'安', re.split('安|\('.decode('utf8'), code)[1]+'级', code.encode('utf8'), Date_F(item['validityDate'])]
            lines.append(line)
        elif type == 'zj':
            tp = {'建':'土建', '水':'水利', '交公':'公路'}
            p = re.split('\[|〔|【|［|（|\(|{|『|「'.decode('utf8'), item['certificateCode'])[0].encode('utf8')
            if p.find('建造')>=0 or p.find('建')>=0 or item['certificateCode'].find('建')>=0: p = '建'
            if p.find('公路')>=0 or p.find('交公')>=0 or p.find('交工')>=0: p = '交公'
            if p not in tp: p = '建'
            line = ['造价工程师', tp[p], '', item['certificateCode'], item['validityDate']]
            lines.append(line)
        elif type == 'zjy':
            line = ['造价员', '', '', item['certificateCode'], item['validityDate']]
            lines.append(line)
        elif type == 'aq':
            temp = { '其他安全':'', '危险物品安全':'', '煤矿安全':'', '非煤矿矿山安全':'', '建筑施工安全':''}
            tp = item['registeredType']
            lv = ""
            if tp.find('其他安全')>=0:
                lv = re.split('\(|\)', tp)[1].encode('utf8')
                lvs = ['农业','水利','电力','消防','交通','其他']
                if lv not in dict([[l, 1] for l in lvs]): lv = '其他'
                tp = '其他安全'
            if tp.find('危险物品安全')>=0: tp = '危险物品安全'
            if tp.encode('utf8') not in temp: return []
            line = ['注册安全工程师', tp, lv, item['engineerCode'], Date_F(item['validityDate'])]
            lines.append(line)
        return lines 
            
    #--------------------------------------------------------------------------------
    #处理建造师过程
    def jianzaoshi_process(self, item, lsNames, personDic):
        ES_Dict = {'companyname':{}, 'certificate':{}}
        common = [['name', 'name'], ['location', 'location'], ['personId', 'personId']] #, ['companyName', 'companyname']
        tp = self.Type.getType(item)
        if not tp:
            
#            print item['personType']
#            cout( item)
            return 1
        
        for c in common: self.AddItem2Dict(item, ES_Dict, c[0], c[1])
        lines = self.typeDeal(tp, item)
#        print tp
#        print lines 
        if not lines: print "Error" 
        for line in lines:
            lmd5 = ','.join(line)
            ES_Dict['companyname'][item['companyName']] = 1
            personId = item['personId']     #人员唯一标识
#            if item['name'] not in lsNames: lsNames[item['name']] = {'companyName':{item['companyName']:personId}, 'code':{item['certificateCode']:personId} }
#            else:
#                if item['companyName']!='':lsNames[item['name']]['companyName'][item['companyName']] = personId 
#                if item['certificateCode']!='':lsNames[item['name']]['code'][item['certificateCode']] = personId
            if personId not in personDic:   #是否收录，同一人员
                ES_Dict['certificate'][lmd5] = line
                personDic[personId] = ES_Dict
            else:
                if lmd5 not in personDic[personId]['certificate']: #防重复
                    personDic[personId]['certificate'][lmd5] = line
                
            
    # 把记录写入搜索引擎
    def ReadMongoTable(self):
        con1 = MongoClient('localhost', 27017)
        con2 = MongoClient('192.168.3.45', 27017)
        con3 = MongoClient('171.221.173.154', 27017)
        db1 = con1['middle']
        db2 = con2['constructionDB']
        db3 = con3['jianzhu3']
        
        write = db2.person5
        self.writer = write 
        write1 = db2.person2
        
        company_table = db1.companyInfo
        jianzaoshi_table = db2.personnelInPCopy
        jianzaoshi2_table = db2.personnelEnterPCopy
        ankaozheng_table = db2.WCSafetyEngineer
        anquangongchengshi_table = db2.safetyEngineer
        zaojiashi_table = db2.CERegistered
        zhuce_table = db2.WCEngineer
        
        index = 0
        personDic = {}
        lsNames = {}
        
#        #**************************************************************************************
#        print 'deal jianzaoshi_table..'
#        # 写建设厅、建造师数据
#        for item in jianzaoshi_table.find():
#            if 0 == self.jianzaoshi_process(item, lsNames, personDic):break
#            index +=1
##            if index>100000:break
#        print 'collect ', len(personDic), 'items.'
#        return

#        #**************************************************************************************
#        print 'deal jianzaoshi2_table..'
#        for item in jianzaoshi2_table.find():
#            if 0 == self.jianzaoshi_process(item, lsNames, personDic):break
#        print 'collect ', len(personDic), 'items.'
#        for p in personDic:  print p, personDic[p]
#        return        
        
#        #**************************************************************************************
#        print 'deal ankaozheng_table..'
#        # 写安考证
#        for item in ankaozheng_table.find():
#            item['name'] = item['engineerName']
#            item['location'] = "四川省"
#            item['personId'] = 'idcard-'+ item['idcard']
#            item['companyName'] = item['workUnits']
#            if 0 == self.jianzaoshi_process(item, lsNames, personDic):break
#        print 'collect ', len(personDic), 'items.'
#        return
    
#        #**************************************************************************************
#        print 'deal anquangongchengshi_table..'        
#        #写安全工程师
#        common = [['engineerName', 'name'], ['engineerCode', 'certificateCode'], ['workUnits', 'companyName']]        
#        for item in anquangongchengshi_table.find():
#            for c in common: item[c[1]] = item[c[0]]
#            item['location'] = "四川省" if item['companyType'] == "省内企业" else ""
#            item['personId'] = 'safe-engineerCode'+ item['engineerCode']
#            if 0 == self.jianzaoshi_process(item, lsNames, personDic):break
#        print 'collect ', len(personDic), 'items.'
#        return

#        #**************************************************************************************
#        print 'deal anquangongchengshi_table..'        
#        #造价工程师
#        common = [['engineerName', 'name'], ['registeredNumb', 'certificateCode'], ['registeredCompany', 'companyName'], ['validDate', 'validityDate'], ['registeredAgencies', 'location']]
#        for item in zaojiashi_table.find():
#            for c in common: item[c[1]] = item[c[0]]  
#            item['personId'] = 'ce-registeredNumb'+ item['registeredNumb']
#            if item['certificateCode'].find('建[造]')!=0: item['certificateCode'] = item['certificateCode'].replace('建〔造〕', '建[造]')
#            if 0 == self.jianzaoshi_process(item, lsNames, personDic):break
#        print 'collect ', len(personDic), 'items.'
#        return
    
        #**************************************************************************************
        print 'deal anquangongchengshi_table..'        
        #写安全工程师
        st =set()
        common = [['qualificationCertNum', 'certificateCode'], ['workingCompany', 'companyName'], ['personType', 'type']]        
        for item in zhuce_table.find():
            for c in common: item[c[1]] = item[c[0]]
            item['validityDate'] = ''
            item['professional'] = ''
            item['location'] = ''
            item['staffLevel'] = ''
            if len(item['gridViewPersonAptitudes'])>0: 
                item['professional'] = item['gridViewPersonAptitudes'][0]['certificateProfessional']
                item['validityDate'] = item['gridViewPersonAptitudes'][0]['expiryDate']
                item['staffLevel'] = item['gridViewPersonAptitudes'][0]['level']
                item['certificateCode'] = item['gridViewPersonAptitudes'][0]['certificateNumber']
            item['personId'] = 'idcard-'+ item['idCard']
            if 0 == self.jianzaoshi_process(item, lsNames, personDic):break
            st.add(item['personType'])
        print 'collect ', len(personDic), 'items.'
        for it in st: print it
        return
        
        
        return
        index = 50000000
        for p in personDic:
            personDic[p]['companyname'] = personDic[p]['companyname'].keys() 
            ls = personDic[p]['certificate'].values()
            personDic[p]['certificate'] = []
            for c in ls: 
                personDic[p]['certificate'].append({'name':c[0], 'professional':c[1], 'level':c[2], 'code':c[3], 'validityDate':c[4]})
            index += 1
            personDic[p]['id'] = str(index)
            personDic[p]['label'] = ''
            personDic[p]['other'] = ''
        
        print len(personDic)
#        for line in personDic: write1.insert(personDic[line])
#        write1.insert(personDic.values())
        return
        '''
        #8月18日加班完成以上功能
        #以下内容已弃用，
        '''

        #**************************************************************************************
        print 'deal zaojiashi_table..'
        #写造价工程师
        common = [['engineerName', 'name'], ['engineerGender', 'gender'], ['branchCompany', 'branchcompany'], ['registeredAgencies', 'zj_registeredagencies'], ['status', 'zj_code'], ['registeredNumb', 'zj_code']]
        for item in zaojiashi_table.find():
            self.common_process(common, item, lsNames, personDic, 'registeredNumb', 'registeredNumb', 'registeredCompany')
        print 'collect ', len(personDic), 'items.'
#        return

        return
 
     
    #--------------------------------------------------------------------------------
    #通用处理过程合并
    def common_process(self, common, item, lsNames, personDic, card, code, company):
        ES_Dict = {'companyname':{}}
        for c in common: self.AddItem2Dict(item,ES_Dict, c[0], c[1])
        ES_Dict['companyname'][item[company]] = 1
        name = ES_Dict['name']
        personId = 're' + item[card]
        if name not in lsNames: 
            lsNames[name] = {'companyName':{item[company]:personId}, 'code':{item[code]:personId} }
            personDic[personId] = ES_Dict
        else:   #名字出现过
            if item[code] in lsNames[name]['code']:  #先核对证书
                return 0
            if item[company] in lsNames[name]['companyName']:  #后核对公司
                personId = lsNames[name]['companyName'][item[company]]
                for e in ES_Dict: personDic[personId][e] = ES_Dict[e]
#                print name, item[company]
#                print personId,personDic[personId]['name'],
#                cout(personDic[personId]['companyname'])
        return ES_Dict       
            
    def dealPerson(self):
        #        connection = pymongo.Connection('192.168.3.45', 27017)
        connection = MongoClient('192.168.3.45', 27017)
        db = connection['constructionDB']
        con = MongoClient('localhost', 27017)
        db2 = con['middle']
        write = db2.person
        self.writer = write 
        con1 = MongoClient('171.221.173.154', 27017)
        db1 = con1['jianzhu']
        write1 = db1.person5
        
        company_table = db.EInPQualification
        ankaozheng_table = db.WCSafetyEngineer
        anquangongchengshi_table = db.safetyEngineer
        
        ankaozheng_table = db.WCSafetyEngineer
        anquangongchengshi_table = db.safetyEngineer
        
        index = 0
        lsAll = []
        personDic = {}
        lsNames = {}
        lsInfo = {}
        
        lsPerson = {}
        index = db2.person5.find().count() + 50000000 + 1
        for item in db2.person5.find():
            name = item['name']
            cpname = item['companyname'][0]
            personId = item['personId']
            lsPerson[personId] = item
            if name not in lsNames: 
                lsNames[name] = {'companyname': { cpname: personId }}
            else:
                if cpname not in lsNames[name]['companyname']: lsNames[name]['companyname'][cpname] = personId
        
        temp = { '其他安全':'', '危险物品安全':'', '煤矿安全':'', '非煤矿矿山安全':'', '建筑施工安全':''}
        lsIf = {}
        
        #处理安证
        for item in db.safetyEngineer.find():
            tp = item['registeredType']
            pname = item['engineerName']
            cpname = item['workUnits']
            if tp == '': continue
            lv = ""
            if tp.find('其他安全')>=0:
                lv = re.split('\(|\)', tp)[1].encode('utf8')
                lvs = ['农业','水利','电力','消防','交通','其他']
                if lv not in dict([[l, 1] for l in lvs]): lv = '其他'
                tp = '其他安全'
            if tp.find('危险物品安全')>=0: tp = '危险物品安全'
            if tp.encode('utf8') not in temp: continue
            ctf = {'name':'注册安全工程师', 'professional':tp, 'level':lv, 'code':item['engineerCode'], 
                   'validityDate': Date_F(item['validityDate'])}
            
            personId = 'safe-engineerCode'+ item['engineerCode']
            #无记录 但 有名字且公司名相同
            if personId not in lsPerson:
                if pname in lsNames and cpname in lsNames[pname]['companyname']:    #可与原记录合并
                    personId = lsNames[pname]['companyname'][cpname]
                    lsPerson[personId]['certificate'].append(ctf)
                else:   #新纪录
                    lsPerson[personId] = {'id':str(index),'name':pname, 'companyname':[cpname], 'label':'', 'other':'', 'personId':personId, 'certificate':[ctf]}
                    index += 1
            else: 
                lsPerson[personId]['certificate'].append(ctf)
#        write.insert(lsPerson.values())
        
#        out( lsInfo)
        return
        index = 50000000
        for p in personDic:
            personDic[p]['companyname'] = personDic[p]['companyname'].keys() 
            ls = personDic[p]['certificate'].values()
            personDic[p]['certificate'] = []
            for c in ls:
                personDic[p]['certificate'].append({'name':c[0], 'professional':c[1], 'level':c[2], 'code':c[3], 'validityDate':c[4]})
            index += 1
            personDic[p]['id'] = str(index)
            personDic[p]['label'] = ''
            personDic[p]['other'] = ''
        write1.insert(personDic.values())
        return

        #**************************************************************************************
        print 'deal ankaozheng_table..'
        # 写安考证
        common = [['engineerName', 'name'], ['engineerGender', 'gender'], ['engineerTitle', 'title'], ['idcard', 'idcard'], ['certificateCode', 'ak_code'], ['certificateStatus', 'ak_status'], ['validityDate', 'ak_validityDate'], ['companyType', 'ak_companytype']]
        for item in ankaozheng_table.find():
            self.common_process(common, item, lsNames, personDic, 'idcard', 'certificateCode', 'workUnits')
        print 'collect ', len(personDic), 'items.'
#        return
        
        #**************************************************************************************
        print 'deal anquangongchengshi_table..'        
        #写安全工程师
        common = [['engineerName', 'name'], ['engineerGender', 'gender'], ['companyType', 'aq_companytype'], ['registeredType', 'aq_registeredtype'], ['validityDate', 'aq_validitydate'], ['engineerCode', 'aq_code']]
        for item in anquangongchengshi_table.find():
            self.common_process(common, item, lsNames, personDic, 'engineerCode', 'engineerCode', 'workUnits')
        print 'collect ', len(personDic), 'items.'
#        return
    
        #**************************************************************************************
        print 'deal zaojiashi_table..'
        #写造价工程师
        common = [['engineerName', 'name'], ['engineerGender', 'gender'], ['branchCompany', 'branchcompany'], ['registeredAgencies', 'zj_registeredagencies'], ['status', 'zj_code'], ['registeredNumb', 'zj_code']]
        for item in zaojiashi_table.find():
            self.common_process(common, item, lsNames, personDic, 'registeredNumb', 'registeredNumb', 'registeredCompany')
        print 'collect ', len(personDic), 'items.'
#        return
        
        for p in personDic:
            personDic[p]['companyname'] = personDic[p]['companyname'].keys()
        write.insert(personDic.values())
        return
        
        #写公司资质
        common = [['qualificationType', 'company_qualificationType'], ['professionalType', 'company_professionalType'], ['professionalLevel', 'company_professionalLevel']]
        for item in  company_table.find():
            ES_Dict = {}
            for c in common: self.AddItem2Dict(item,ES_Dict, c[0], c[1])
        
        return
    
        #写公司（这里会出现重名BUG，但不管啦）
#        res = self.es.index(index="jh-index", doc_type='jh-type',id=(ES_Dict["companyname"] + "#" + ES_Dict["name"]), body=ES_Dict)

#统计
def select():
    #        connection = pymongo.Connection('192.168.3.45', 27017)
    con = MongoClient('localhost', 27017)
    db2 = con['middle']
    person = db2.person
    company = db2.companyInfo
    
    keyp = ['注册建造师', '安考证', '造价工程师', '造价员', '注册安全工程师']
    for k in keyp: print k, ': ', person.find({'certificate.name':k }).count()
    #打印同时多个证人数矩阵
    print '\t',
    for k in keyp: print k,'\t',
    print
    for i in range(len(keyp)):
        print keyp[i],'\t',
        for j in range(len(keyp)): print person.find({'$and':[{'certificate.name':keyp[i]},{'certificate.name':keyp[j]}]}).count(),'\t',
        print
    
    return
    lsA = {}
#    for item in company.find():
#        for line in item['qualificationType']:
#            if line['name'] not in lsA: lsA[line['name']] =1
#            else: lsA[line['name']] +=1
#    cout(lsA)
#    #公司资质统计
#    a = ['设计与施工一体化','专业承包','总承包','园林绿化']
    for item in company.find():
        for line in item['qualificationType']:
            if line['name'] not in lsA: lsA[line['name']] = {}
            for ln in line['professionalType']:
                if ln['name'] not in lsA[line['name']]: lsA[line['name']][ln['name']] = {}
                else: #等级
                    for lv in ln['professionalLevel']:
                        if lv not in lsA[line['name']][ln['name']]: lsA[line['name']][ln['name']][lv] = 1
                        else: lsA[line['name']][ln['name']][lv] += 1
    for ll in lsA:
        print ll
        for l in lsA[ll]:
            print '\t', l
            for t in lsA[ll][l]: print '\t\t', t, '\t', lsA[ll][l][t]     
    return
    cIndex = 0
    bIndex = 0
    gIndex = 0
    qIndex = 0
    for item in company.find():
        if 'courtRecords' in item and len(item['courtRecords'])>0: cIndex += 1
        if len(item['badbehaviors']['badBehaviorDetails'])>0: bIndex += 1
        if len(item['goodbehaviors'])>0: gIndex += 1
        if len(item['qualificationType'])>0: qIndex += 1
    print '诉讼记录', cIndex    
    print '不良记录', bIndex    
    print '优良记录', gIndex
    print '公司资质', qIndex
    print '建造师', company.find({'certificate.type':'建造师'}).count()
    print '无资质公司', company.find({'qualificationType':[]}).count()
    
    cout(lsA)
#    keyc = ['施工总承包', '专业承包', '', '造价员', '注册安全工程师']
    
#        print
    
#    for k in keyp: print k, ': ', person.find({'certificate.name':k }).count()
#    print person.find({'certificate.name':'安考证', 'certificate.name':'安考证'}).count()
#    print person.find({'certificate.name':'安考证', 'certificate.name':'造价员'}).count()

#    for item in person.find({'certificate':{'name':}}):
        
    
    
def Code_F(dic):
    return dict([[k.decode('gb18030'), dic[k].decode('gb18030')] for k in dic.keys()])
    

def Date_F(str):
    str = str.decode('utf8')
    if not str or str == '': return ''
    if not (str.find("年")>0 and str.find("月")>0): return str.encode('utf8')
    sp = re.split("年|月".decode('utf8'), str)
    return (sp[0] + '-' + sp[1] + '-' + sp[2].replace('日', '')).encode('utf8')

print 'Hello Moto..'
dt = datetime.datetime.now()

md = CReadData2ES()
md.ReadMongoTable()


#md.dealPerson()
#select()

#md.company_process()
#md.company_certificate()
#md.person_deal()
#print Date_F('2012年02月23日')
#print Date_F('2012-02-03')


print datetime.datetime.now(), datetime.datetime.now()-dt
print 'The End!'
