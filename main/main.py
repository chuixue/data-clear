#encoding:gb18030
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
reload(sys)
sys.setdefaultencoding( "gb18030" )
import traceback

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
   
class CReadData2ES(object):

    def __init__(self):
#        self.es = Elasticsearch()
        pass

    def IS_JianZaoShi(self,item):
        str = item["type"]
        if(str.find('建造')==-1):
            return False;
        else:
            return True;

    def IS_AnKaoZheng(self, item):
        str = item["certificateCode"]
        if (str.find('建') >=0 and str.find('安') >=0 ):
            return True;
        else:
            return False;

    def IS_ZaoJiaGongChengShi(self, item):
        str = item["certificateCode"]
        if (str.find('造') >= 0):
            return True;
        else:
            return False;

    def IS_ZaoJiaYuan(self, item):
        str = item["certificateCode"]
        strType = item["type"]
        if (str.find('造') == -1 and strType.find("造价工程师")>=0):
            return True;
        else:
            return False;

    def AddItem2Dict(self,item,dict,itemkey,dictkey):
        if item[itemkey]!="":
            dict[dictkey] = item[itemkey];
        return
    
    def _getType(self, _item):
        if self.IS_JianZaoShi(_item):return 'jz'
        elif self.IS_AnKaoZheng(_item):return 'ak'
        elif self.IS_ZaoJiaGongChengShi(_item):return 'zj'
        elif self.IS_ZaoJiaYuan(_item):return 'zjy'
    
#    #--------------------------------------------------------------------------------
#    #处理建造师过程
#    def jianzaoshi_process(self, item, lsNames, personDic):
#        ES_Dict = {'companyname':{}}
#        common = [['name', 'name'], ['location', 'location'], ['post', 'workpost']] #, ['companyName', 'companyname']
#        m = {'jz':[['certificateCode', 'jz_code'], ['validityDate', 'jz_validityDate'],['staffLevel', 'jz_staffLevel'], ['type', 'jz_type']],
#              'ak':[['certificateCode', 'ak_code'], ['validityDate', 'ak_validityDate']],
#              'zj':[['certificateCode', 'zj_code'], ['validityDate', 'zj_validityDate'],['staffLevel', 'zj_staffLevel'], ['type', 'zj_type']],
#              'zjy':[['certificateCode', 'zjy_code'], ['validityDate', 'zjy_validityDate'],['staffLevel', 'zjy_staffLevel'], ['type', 'zjy_type']]
#        }
#        tp = self._getType(item)
#        if not tp: return 1
#        for c in common: self.AddItem2Dict(item,ES_Dict, c[0], c[1])
#        for c in m[tp]: self.AddItem2Dict(item,ES_Dict, c[0], c[1])
#        ES_Dict['companyname'][item['companyName']] = 1
#        personId = item['personId']     #人员唯一标识
#        if item['name'] not in lsNames: lsNames[item['name']] = {'companyName':{item['companyName']:personId}, 'code':{item['certificateCode']:personId} }
#        else:
#            if item['companyName']!='':lsNames[item['name']]['companyName'][item['companyName']] = personId 
#            if item['certificateCode']!='':lsNames[item['name']]['code'][item['certificateCode']] = personId
#        if personId not in personDic:   #是否收录，同一人员
#            personDic[personId] = ES_Dict
#        else:
#            if (tp + '_code') not in personDic[personId]:
#                for e in ES_Dict: personDic[personId][e] = ES_Dict[e]
##            return 0
#        
#    #--------------------------------------------------------------------------------
#    #通用处理过程合并
#    def common_process(self, common, item, lsNames, personDic, card, code, company):
#        ES_Dict = {'companyname':{}}
#        for c in common: self.AddItem2Dict(item,ES_Dict, c[0], c[1])
#        ES_Dict['companyname'][item[company]] = 1
#        name = ES_Dict['name']
#        personId = 're' + item[card]
#        if name not in lsNames: 
#            lsNames[name] = {'companyName':{item[company]:personId}, 'code':{item[code]:personId} }
#            personDic[personId] = ES_Dict
#        else:   #名字出现过
#            if item[code] in lsNames[name]['code']:  #先核对证书
#                return 0
#            if item[company] in lsNames[name]['companyName']:  #后核对公司
#                personId = lsNames[name]['companyName'][item[company]]
#                for e in ES_Dict: personDic[personId][e] = ES_Dict[e]
##                print name, item[company]
##                print personId,personDic[personId]['name'],
##                cout(personDic[personId]['companyname'])
#        return ES_Dict
#    

    
    #公司人员资质处理
    def company_certificate(self):
        connection = MongoClient('192.168.3.45', 27017)
        db = connection['Ent_Person']
        con = MongoClient('localhost', 27017)
        db2 = con['middle']
        company = db2.company2
        write = db2.companyInfo5
        provence = db.EInProvenceDetail
        companyDic = {}
        certificates = {}   #公司资质备份
        for item in provence.find():
            if item['companyName'].strip() == '' : continue
            certificates[item['companyName']] = item['certificates']
            companyDic[item['companyName']] = {'company_name':item['companyName'], 'certificate':[], 'companyachievement':[], 
                'label':0, 'companyBases':item['companyBases'][0], 'company_id':item['entId'], 
                'goodbehaviors':item['goodBehaviors'], 'badbehaviors':item['badBehaviors'], 'other':''}
            for p in item['enterpriseStaffs']: 
                tp = {'personID':p['personID'], 'name':p['personName'], 'type':p['certificateType'], 'code':p['certificateNumber'], 'level':'' }
                companyDic[item['companyName']]['certificate'].append(tp)
        index = 0
        ls = {}
        tpKey = [['label', 0], ['other', ''], ['companyachievement',[]], ['badbehaviors',[]], ['goodbehaviors',[]]]
        
        for item in company.find(): #来自公司资质表
            for k in tpKey: item[k[0]] = k[1] 
            ls[item['company_name']] = item
            if item['company_name'] in companyDic: 
                for e in companyDic[item['company_name']]:ls[item['company_name']][e] = companyDic[item['company_name']][e]
            else:
                if 'certificate' not in ls[item['company_name']]: ls[item['company_name']]['certificate'] = []
                if 'companyBases' not in ls[item['company_name']]: ls[item['company_name']]['companyBases'] = {}
        print len(ls), len(companyDic), index
        return
        index = 10000000
        for cpname in companyDic:
            if 'company_id' not in companyDic[cpname]:
                print "!"
            if cpname not in ls:
                ls[cpname] = companyDic[cpname]
                if 'qualificationType' not in ls[cpname]: ls[cpname]['qualificationType'] = []
                ls[cpname]['company_qualification'] = ','.join(map(lambda x:str(x['qc_qualification']) if x['qc_qualification'] else '', certificates[cpname])).decode('gb18030')
        
        #加id 最后的处理  
        for cp in ls:
            ls[cp]['id'] = str(index)
            if 'company_qualification' not in ls[cp]: ls[cp]['company_qualification'] = "" #填补字段
            tps = set(re.split(',', ls[cp]['company_qualification'])) #去重
            tps =  ','.join([t for t in tps if t.strip()!='']) #
            ls[cp]['company_qualification'] = tps
            if 'companyBases' not in ls[cp]: print '!'
            if 'organizationCode' in ls[cp]['companyBases'] and 'legalRepresentative' in ls[cp]['companyBases']:
                ccode = ls[cp]['companyBases']['organizationCode']
                if not haveNum(ccode):
#                    print ccode, '____', ls[cp]['companyBases']['legalRepresentative']
                    ls[cp]['companyBases']['organizationCode'] = ls[cp]['companyBases']['legalRepresentative'] 
                    ls[cp]['companyBases']['legalRepresentative'] = ccode
                pass
            
            index += 1
       
        print len(ls)
#        write.insert(ls.values())
                
    #公司资质处理
    def company_process(self):
        connection = MongoClient('192.168.3.45', 27017)
        db = connection['constructionDB']
        con = MongoClient('localhost', 27017)
        db2 = con['middle']
        write = db2.company2
        company_table = db.EInPQualification
        company2_table = db.EInPProvenceDetail
        companyDic = {}
        common = [['companyName', 'company_name'], #['professionalType', 'company_professionalType'], 
                  ['entId', 'company_id'], ['qc_qualification', 'company_qualification']]
        for item in  company_table.find():
            ES_Dict = {}
            for c in common: self.AddItem2Dict(item,ES_Dict, c[0], c[1])
            if item['entId'] not in companyDic:
                ES_Dict['qualificationType'] = {item['qualificationType'] : {item['professionalType']:{ item['professionalLevel']:1}}}
                companyDic[item['entId']] = ES_Dict
            else:   #公司
                companyDic[item['entId']]['company_qualification'] += ',' + ES_Dict['company_qualification']
                if item['qualificationType'] not in companyDic[item['entId']]['qualificationType']:  #一级
                    companyDic[item['entId']]['qualificationType'][item['qualificationType']] = { item['professionalType']:{ item['professionalLevel']:1}}
                else:
                    if item['professionalType'] not in companyDic[item['entId']]['qualificationType'][item['qualificationType']]:    #二级
                        companyDic[item['entId']]['qualificationType'][item['qualificationType']][item['professionalType']] = {item['professionalLevel']:1}
                    else:
                        if item['professionalLevel'] not in companyDic[item['entId']]['qualificationType'][item['qualificationType']][item['professionalType']]:
                            companyDic[item['entId']]['qualificationType'][item['qualificationType']][item['professionalType']][item['professionalLevel']] = 1
        pass
#        write.insert(companyDic.values())
#        return
        for l in companyDic:
            ls = map(lambda x:{'name':x}, companyDic[l]['qualificationType'].keys())
#            if len(ls)>1:print companyDic[l]['company_name'] 
            for n in ls:
                n['professionalType'] = map(lambda x:{'name':x, 'professionalLevel':companyDic[l]['qualificationType'][n['name']][x].keys()}, 
                    companyDic[l]['qualificationType'][n['name']].keys())
            companyDic[l]['qualificationType'] = ls 
        write.insert(companyDic.values())

#    def person_deal(self):
#        con = MongoClient('localhost', 27017)
#        db = con['middle']
#        person = db.person
#        write = db.person3
#        head = ['jz_', 'zj_', 'zjy_', 'ak_', 'aq_']
#        common = ['_id', 'idcard', 'companyname', 'location', 'name', 'workpost', 'branchcompany', 'title', 'gender']
#        head_dic = {}
#        for h in head: head_dic[h] = 1
#        lines = []
#        for item in person.find():    
#            line = {}
#            for v in common:
#                if v in item: line[v] = item[v]
#                else: line[v] = ''
#            line['certificate'] = {}
#            for v in item:
#                if v not in common: 
#                    sp = v.split('_')
#                    cls = sp[0] + '_'
#                    if cls not in head_dic: continue
#                    if sp[1] == "Code": sp[1] = "code"
#                    if cls not in line['certificate']: line['certificate'][cls] = {sp[1] : item[v]}
#                    else: line['certificate'][cls][sp[1]] = item[v]
#                    if sp[1] == 'validityDate' or sp[1] == 'validitydate': line['certificate'][cls]['validityDate'] = Date_F(item[v])
#            lines.append(line)
#        for l in lines:
#            l['certificate'] = l['certificate'].values()
##            print l
#
#        write.insert(lines)
#        return
#        
#        ldic = {}
#        for item in person.find():    
#            for v in item:
#                if v not in ldic: ldic[v] = 1
#                
#        for l in ldic:
#            print l
##        write.insert()

def Date_F(str):
    str = str.decode('gb18030')
    if not str or str == '': return ''
    if not (str.find("年")>0 and str.find("月")>0): return str
    sp = re.split("年|月".decode('gb18030'), str)
    return sp[0] + '-' + sp[1] + '-' + sp[2].replace('日', '')


print 'Hello Moto..'
dt = datetime.datetime.now()

md = CReadData2ES()
#md.ReadMongoTable()
#md.company_process()
md.company_certificate()
#md.person_deal()
#print Date_F('2012年02月23日')
#print Date_F('2012-02-03')



print datetime.datetime.now(), datetime.datetime.now()-dt
print 'The End!'


