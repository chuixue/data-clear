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

    def typeDeal(self, type, item):
        lines = []
        ps = re.split(',|、'.decode('utf8'), item['professional'])
        if type == 'jz':
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
            line = ['造价工程师', p, '', item['certificateCode'], item['validityDate']]
            lines.append(line)
        elif type == 'zjy':
            line = ['造价员', '', '', item['certificateCode'], item['validityDate']]
            lines.append(line)
        elif type == 'aq':
            print item['type'], item['staffLevel'], item['professional'], item['validityDate'], item['certificateCode']
            
            return 
            line = ['造价员', '', '', item['certificateCode'], item['validityDate']]
            lines.append(line)
        return lines 
            
    #--------------------------------------------------------------------------------
    #处理建造师过程
    def jianzaoshi_process(self, item, lsNames, personDic):
        ES_Dict = {'companyname':{}, 'certificate':{}}
        common = [['name', 'name'], ['location', 'location'], ['post', 'workpost'],
                    ['personId', 'personId']] #, ['companyName', 'companyname'] 
        tp = self._getType(item)
        if not tp: return 1
        for c in common: self.AddItem2Dict(item,ES_Dict, c[0], c[1])
        lines = self.typeDeal(tp, item)
        for line in lines:
            lmd5 = ','.join(line)
            ES_Dict['companyname'][item['companyName']] = 1
            personId = item['personId']     #人员唯一标识
            if item['name'] not in lsNames: lsNames[item['name']] = {'companyName':{item['companyName']:personId}, 'code':{item['certificateCode']:personId} }
            else:
                if item['companyName']!='':lsNames[item['name']]['companyName'][item['companyName']] = personId 
                if item['certificateCode']!='':lsNames[item['name']]['code'][item['certificateCode']] = personId
            if personId not in personDic:   #是否收录，同一人员
                ES_Dict['certificate'][lmd5] = line
                personDic[personId] = ES_Dict
            else:
                if lmd5 not in personDic[personId]['certificate']:
                    personDic[personId]['certificate'][lmd5] = line
                
    
    # 把记录写入搜索引擎
    def ReadMongoTable(self):
#        connection = pymongo.Connection('192.168.3.45', 27017)
        
        connection = MongoClient('192.168.3.45', 27017)
        db = connection['constructionDB']
        con = MongoClient('localhost', 27017)
        db2 = con['middle']
        write = db2.person5
        self.writer = write 
        con1 = MongoClient('171.221.173.154', 27017)
        db1 = con1['jianzhu']
        write1 = db1.person
        
        company_table = db.EInPQualification
        jianzaoshi_table = db.personnelInPCopy
        jianzaoshi2_table = db.personnelEnterPCopy
        ankaozheng_table = db.WCSafetyEngineer
        anquangongchengshi_table = db.safetyEngineer
        zaojiashi_table = db.CERegistered
        
        index = 0
        lsAll = []
        personDic = {}
        lsNames = {}
        
        #**************************************************************************************
        print 'deal jianzaoshi_table..'
        # 写建设厅、建造师数据
        for item in jianzaoshi_table.find():
            if 0 == self.jianzaoshi_process(item, lsNames, personDic):break
            index +=1
#            if index>100:break
        print 'collect ', len(personDic), 'items.'
#        return

        #**************************************************************************************
        print 'deal jianzaoshi2_table..'
        for item in jianzaoshi2_table.find():
            if 0 == self.jianzaoshi_process(item, lsNames, personDic):break
        print 'collect ', len(personDic), 'items.'
        
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
#md.company_process()
#md.company_certificate()
#md.person_deal()
#print Date_F('2012年02月23日')
#print Date_F('2012-02-03')


print datetime.datetime.now(), datetime.datetime.now()-dt
print 'The End!'
