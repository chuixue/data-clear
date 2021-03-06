#encoding:utf8
'''
Created on Sep 1, 2016

@author: Administrator
'''

import pymongo
from pymongo import MongoClient
import datetime
import time
import re
import string
import numpy as np
import public as P
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

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
        elif self.IS_JIANZHUSHI(_item):return 'jzs'
        elif self.IS_JIANLI(_item):return 'jl'
        elif self.IS_KANCHA(_item):return 'kc'
        elif self.IS_JIEGOU(_item):return 'jg'
        elif self.IS_ZYJS(_item):return 'zyjs'
        elif self.IS_OTHER(_item):return 'other'
        
        
    def IS_JianZaoShi(self,item):   #注册建造师
        if "type" in item and item["type"].find('建造')!=-1: return True
        return False
    
    def IS_AnKaoZheng(self, item):  #安考证
        if "certificateCode" in item: 
            str = item["certificateCode"]
            if (str.find('建') >=0 and str.find('安') >=0 ): return True;
            if (str.find('水') >=0 and str.find('安') >=0 ): return True;
            if (str.find('交') >=0 and str.find('安') >=0 ): return True;
        return False;
    
    def IS_ZaoJiaGongChengShi(self, item):  #造价师
        if 'personType' in item and item['personType']=='造价工程师': return True
        if "certificateCode" in item:
            if 'staffLevel' in item and item['staffLevel'].find("造价工程师")>=0: return True
            if (item["certificateCode"].find('造') >= 0): return True; 
        else:
            if 'staffLevel' in item and 'type' in item and item['type'].find("造价工程师")>=0 and item['staffLevel'].find("造价工程师")>=0: return True
        return False;
    
    def IS_ZaoJiaYuan(self, item):  #
        if 'personType' in item and item['personType']=='造价员': return True
        if 'certificateCode' in item and 'type' in item:
            if (item["certificateCode"].find('造') == -1 and item["staffLevel"].find("造价员")>=0): return True
        return False
        
    def IS_ZhuCeAnQuan(self, item):
        if 'registeredType' in item and item['registeredType'].find('安全')>=0: return True
        return False
    
    def IS_JIANLI(self, item):  #监理工程师
        if 'type' in item and item['type']=="注册监理工程师": return True
        if 'type' in item and item['type']=="总监理工程师": return True
    
    def IS_JIANZHUSHI(self, item):  #注册建筑师
        if 'type' in item and item['type']=="注册建筑师": return True
    
    def IS_JIEGOU(self, item):  #注册结构师
        if 'type' in item and item['type']=="注册结构师": return True
        
    def IS_KANCHA(self, item):  #勘察设计师
        if 'type' in item and item['type']=="勘察设计工程师": return True
    
    def IS_ZYJS(self, item):  #专业技术管理人员
        if 'type' in item and item['type']=="专业技术管理人员": return True
        
    def IS_OTHER(self, item):  #其它
        if 'type' in item:
            if item['type']=="特种作业人员": return True            
#            if item['type']=="专业技术管理人员": return True
#            if item['type']=="专职安全生产管理员": return True    #省外无证书
#            if item['type']=="企业主要负责人": return True        #省外无证书

class CReadData2ES(object):

    def __init__(self): self.Type = Type()

    def AddItem2Dict(self,item,dict,itemkey,dictkey):
        if item[itemkey]!="": dict[dictkey] = item[itemkey];
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
                if p!='': p = '' if item['professional'] == '注册建造师' else tp[p] + '工程' 
                if item['staffLevel']=='建筑工程': item['staffLevel']=""    #解决部分数据错乱
                lv = '' if item['staffLevel']=="" else item['staffLevel'].split('级')[0] + '级'
                line = ['注册建造师', p, lv, item['certificateCode'], Date_F(item['validityDate'])]
                lines.append(line)
        elif type == 'ak':
            code = item['certificateCode']
            if code.find('施')!=-1: return []
            p = [k for k in ['水', '交', '建'] if code.find(k)>=0]
            lslv = {'A':'A', 'B':'B', 'C':'C', 'C1':'C1', 'C2':'C2', u"Ａ":"A", u"Ｂ":"B", u"Ｃ":"C", "c":"C"}
            splv = re.split('【|市|审|安|\(|（|\（|\)|）|\[| '.decode('utf8'), code)
            lv = splv[1]
            if lv not in lslv:
                if '辽'==code[0:1] and lv[1:2] in lslv: lv = lv[1:2]
                if '新'==code[0:1] and lv[0:1] in lslv: lv = lv[0:1]
                if '藏'==code[0:1] and lv[0:1] in lslv: lv = lv[0:1]
                if '湘'==code[0:1] and len(splv)==6 and splv[3] in lslv: lv = splv[3] 
                if '鲁'==code[0:1] and lv[0:1] in lslv: lv = lv[0:1]
                if '津'==code[0:1] and splv[2] in lslv: lv = splv[2]
                if '晋'==code[0:1] and splv[2] in lslv: lv = splv[2]
                if '渝'==code[0:1] and len(splv)==4 and splv[2] in lslv: lv = splv[2]
                if '渝'==code[0:1] and len(splv)==2 and splv[1][0:1] in lslv: lv = splv[1][0:1]
                if '苏'==code[0:1] and len(splv)==4 and splv[2][0:1] in lslv: lv = splv[2][0:1]
                if '粤'==code[0:1] and len(splv)==2 and splv[1][0:1] in lslv: lv = splv[1][0:1]
                if '闽'==code[0:1] and len(splv)==2 and splv[1][0:1] in lslv: lv = splv[1][0:1]
                if '闽'==code[0:1] and len(splv)==4 and splv[2] in lslv: lv = splv[2]
                if '琼'==code[0:1] and len(splv)>3 and splv[2] in lslv: lv = splv[2]
                if '赣'==code[0:1] and len(splv)==2 and splv[1][0:1] in lslv: lv = splv[1][0:1]
                if '冀'==code[0:1] and len(splv)==2 and splv[1][0:1] in lslv: lv = splv[1][0:1]
                if '冀'==code[0:1] and len(splv)==4 and splv[1][-1:] in lslv: lv = splv[1][-1:]
                if '甘'==code[0:1] and len(splv)==4 and splv[1][-1:] in lslv: lv = splv[1][-1:]
                if '京'==code[0:1] and len(splv)==3 and splv[1][0:1] in lslv: lv = splv[1][0:1]
                if '京'==code[0:1] and len(splv)==4 and splv[2] in lslv: lv = splv[2]
                if '豫'==code[0:1] and len(splv)>3 and splv[2] in lslv: lv = splv[2]
                if '川'==code[0:1] and len(splv)==2 and splv[1][0:1] in lslv: lv = splv[1][0:1]
                if '川'==code[0:1] and len(splv)==4 and splv[3][0:1] in lslv: lv = splv[3][0:1]
                if '交'==code[0:1] and len(splv)==8 and splv[2] in lslv: lv = splv[2]
                if '水'==code[0:1] and len(splv)==3 and splv[1][0:1] in lslv: lv = splv[1][0:1]
            lv = "" if lv not in lslv else lslv[lv]+'级'
            line = ['安考证', p[0]+'安', lv, code.encode('utf8'), Date_F(item['validityDate'])]
            lines.append(line)
        elif type == 'zj':
            tp = {'建':'土建', '水':'水利', '交公':'公路', '水利':'水利'}
            p = re.split('\[|〔|【|［|（|\(|{|『|「'.decode('utf8'), item['certificateCode'])[0].encode('utf8')
            if p.find('建造')>=0 or p.find('建')>=0 or item['certificateCode'].find('建')>=0: p = '建'
            if p.find('公路')>=0 or p.find('交公')>=0 or p.find('交工')>=0 or p.find('公造')>=0: p = '交公'
            if p.find('水')>=0 or p.find('SL')>=0: p = '水'    
            if 'professional' not in item: item['professional'] = ''
            if item['professional'] in tp: p = item['professional'] 
            if p not in tp: p ='建'
            line = ['造价工程师', tp[p], '', item['certificateCode'], item['validityDate']]
            lines.append(line)
        elif type == 'zjy':
            p = item['professional'].encode('utf8')
            lsp = {'水利':'水利', '公路':'公路', '土建':'土建'}
            if item['certificateCode'].find('水')!=-1: p = '水利'
            if item['certificateCode'].find('公')!=-1: p = '公路'
            if item['certificateCode'].find('建')!=-1: p = '土建'
            p = '土建' if p not in lsp else lsp[p]
            line = ['造价员', p, '', item['certificateCode'], item['validityDate']]
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
        elif type == 'jzs':
            lv = item['staffLevel'].split('级')[0] + '级'
            line = ['注册建筑师', '', lv, item['certificateCode'], Date_F(item['validityDate'])]
            lines.append(line)
        elif type == 'jl':
            pass
        elif type == 'kc':
            names = ['注册化工工程师', '注册土木工程师', '注册电气工程师', '注册公用设备工程师']
            if item['staffLevel'] not in names: return []
            line = [item['staffLevel'], '', '', item['certificateCode'], Date_F(item['validityDate'])]
            lines.append(line)
        elif type == 'jg':
            lv = item['staffLevel'].split('级')[0] + '级'
            line = ['注册结构师', '', lv, item['certificateCode'], Date_F(item['validityDate'])]
            lines.append(line)
        elif type == 'zyjs':
            return []   
            line = [item['post'], item['professional'], '', item['certificateCode'], Date_F(item['validityDate'])]
            lines.append(line)
        return lines 
        
    #--------------------------------------------------------------------------------
    #处理建造师过程
    def jianzaoshi_process(self, item, lsNames, personDic):
        tp = self.Type.getType(item)
        if not tp: 
            return 1
        cpname = item['companyName'].encode('utf8')
        personId = item['personId']     #人员唯一标识
        lines = self.typeDeal(tp, item) 
        for line in lines:
            lmd5 = ','.join(line)
            if personId not in personDic: personDic[personId] = {'companyname':{}, 'certificate':{}, 'name':item['name'], 'location':item['location'], 'personId':personId} 
            personDic[personId]['certificate'][lmd5] = line
            personDic[personId]['companyname'][cpname] = 1
            
                
    def calcPerson(self, lsp):
        print 'collect ', len(lsp), 'items.'
        lsst = {}
        for p in lsp:
            for c in lsp[p]['certificate'].values():
                if c[0] not in lsst: lsst[c[0]] = [set(), set(), 1]
                else:
                    lsst[c[0]][0].add(c[1])
                    lsst[c[0]][1].add(c[2])
                    lsst[c[0]][2] += 1
        for lst in lsst:
            print lst
            print "\t professional:", 
            out(lsst[lst][0])
            print "\t level:", 
            out(lsst[lst][1])
            print "\t count :", 
            print lsst[lst][2]

               
    def dealPerson(self):
        con1 = MongoClient('localhost', 27017)
        con2 = MongoClient('192.168.3.45', 27017)
        con3 = MongoClient('171.221.173.154', 27017)
        db1 = con1['middle']
        db2 = con2['constructionDB']
        db3 = con3['jianzhu3']
        
        write1 = db1.personNew2
#        self.writer = write1 
        
        index = 0
        personDic = {}
        lsNames = {}
        
        #**************************************************************************************
        print 'deal jianzaoshi_table..：personnelInPCopy'
        #含注册土木工程师、安考证、造价工程师、注册化工工程师、造价员、注册建造师、注册电气工程师、注册建筑师、注册公用设备工程师、注册结构师
        for item in db2.personnelInPCopy.find():
            if 0 == self.jianzaoshi_process(item, lsNames, personDic):break
#        self.calcPerson(personDic)
#        return

        #**************************************************************************************
        print 'deal jianzaoshi2_table..：personnelEnterPCopy'
        #含注册土木工程师、安考证、造价工程师、注册化工工程师、造价员、注册结构师、注册电气工程师、注册建筑师、注册公用设备工程师、注册建造师
        for item in db2.personnelEnterPCopy.find():
            if 0 == self.jianzaoshi_process(item, lsNames, personDic):break
#        self.calcPerson(personDic)
#        return        

        #**************************************************************************************
        print 'deal ankaozheng_table..'
        #只有安考证
        for item in db2.WCSafetyEngineer.find():
            item['name'] = item['engineerName']
            item['location'] = "四川省"
            item['idCard'] = item['idcard'].strip()
            item['personId'] = 'idcard-'+ item['idCard'] if item['idCard'] != '' else item['certificateCode']  
            item['companyName'] = item['workUnits']
            if 0 == self.jianzaoshi_process(item, lsNames, personDic):break
#        self.calcPerson(personDic)
#        return
    
        #**************************************************************************************
        print 'deal anquangongchengshi_table: safetyEngineer..'        
        #只有注册安全工程师
        common = [['engineerName', 'name'], ['engineerCode', 'certificateCode'], ['workUnits', 'companyName']]        
        for item in db2.safetyEngineer.find():
            for c in common: item[c[1]] = item[c[0]]
            item['location'] = "四川省" if item['companyType'] == "省内企业" else ""
            item['personId'] = 'safe-engineerCode'+ item['engineerCode']
            if 0 == self.jianzaoshi_process(item, lsNames, personDic):break
#        self.calcPerson(personDic)
#        return

        #**************************************************************************************
        print 'deal zaojiashi_table: CERegistered..'        
        #只有造价工程师
        common = [['engineerName', 'name'], ['registeredNumb', 'certificateCode'], ['registeredCompany', 'companyName'], ['validDate', 'validityDate'], ['registeredAgencies', 'location']]
        for item in db2.CERegistered.find():
            for c in common: item[c[1]] = item[c[0]]  
            item['personId'] = 'ce-registeredNumb'+ item['registeredNumb']
            if item['certificateCode'].find('建[造]')!=0: item['certificateCode'] = item['certificateCode'].replace('建〔造〕', '建[造]')
            if 0 == self.jianzaoshi_process(item, lsNames, personDic):break
#        self.calcPerson(personDic)
#        return
        
        lsCompany = P.getCompanyId(db1.companyInfoNew2)
        #**************************************************************************************
        print 'deal anquangongchengshi_table..: WCEngineer'        
        #含造价工程师、造价员、注册建造师、安考证
        common = [['qualificationCertNum', 'certificateCode'], ['workingCompany', 'companyName'], ['personType', 'type']]
        lsclr = ['validityDate', 'professional', 'location', 'staffLevel']        
        lsn = { '造价员':1, '造价工程师':1, '质量检测员':1, '建造师':1 }
        for item in db2.WCEngineer.find():
            for c in common: item[c[1]] = item[c[0]]
            for c in lsclr: item[c] = ''
            if item['type']=='监理工程师' or item['type']=='总监理工程师': continue
            if len(item['gridViewPersonAptitudes'])>0: 
                item['professional'] = item['gridViewPersonAptitudes'][0]['certificateProfessional']
                item['validityDate'] = item['gridViewPersonAptitudes'][0]['expiryDate']
                item['staffLevel'] = item['gridViewPersonAptitudes'][0]['level']
                item['certificateCode'] = item['gridViewPersonAptitudes'][0]['certificateNumber']
            item['idCard'] = item['idCard'].strip()
            item['personId'] = 'idcard-'+ item['idCard']
            if item['idCard']=='': item['personId'] = 'idcard-'+ item['qualificationCertNum']
            if item['type'].encode('utf8') in lsn: item['professional'] = '水利'       
            if 0 == self.jianzaoshi_process(item, lsNames, personDic):break
#        self.calcPerson(personDic)
#        return

        #************************************写数据*********************************************
        lsCompany = P.getCompanyId(db1.companyInfoNew2)
        index = 50000000
        for p in personDic:
            cps = personDic[p]['companyname'].keys()
            cpname = '' if len(cps)==0 else cps[0].encode('utf8')
            personDic[p]['companyname'] = cpname 
            ls = personDic[p]['certificate'].values()
            personDic[p]['certificate'] = []
            for c in ls: 
                personDic[p]['certificate'].append({'name':c[0], 'professional':c[1], 'level':c[2], 'code':c[3], 'validityDate':c[4]})
            index += 1
            personDic[p]['company_id'] = lsCompany[cpname] if cpname in lsCompany else 0 
            personDic[p]['id'] = index
            personDic[p]['label'] = 0
            personDic[p]['other'] = ''
            personDic[p]['updateTime'] = datetime.datetime.now()
        print len(personDic)
        write1.insert(personDic.values())
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
md.dealPerson()


#md.dealPerson()
#select()

#md.company_process()
#md.company_certificate()
#md.person_deal()
#print Date_F('2012年02月23日')
#print Date_F('2012-02-03')


print datetime.datetime.now(), datetime.datetime.now()-dt
print 'The End!'
