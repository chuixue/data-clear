#coding:utf8
'''
Created on Sep 14, 2016

@author: Administrator
'''
from pymongo import MongoClient
import datetime
import public as P
import libPerson as libP
import copy
import config as CFG
import traceback
import re
import random
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def cout(ls):
    for l in ls: print l, ':', ls[l],
    print
def out(ls):
    for l in ls: print l,
    print
def Code_F(dic):
    return dict([[k.decode('gb18030'), dic[k].decode('gb18030')] for k in dic.keys()])
        
#--------------------------------------------------------------------------------
def calcPerson(lsp):
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
        
#处理建造师过程
def common_process(item, personDic):
    tp = libP.Type.getType(item)
    if not tp: return 1
    cpname = item['companyName'].encode('utf8')
    personId = item['personId']     #人员唯一标识
    lines = libP.dealType(tp, item) 
    for line in lines:
        lmd5 = ','.join(line)
        if personId not in personDic: personDic[personId] = {'companyname':{}, 'certificate':{}, 'name':item['name'], 'location':item['location'], 'personId':personId} 
        personDic[personId]['certificate'][lmd5] = line
        personDic[personId]['companyname'][cpname] = 1
    return personDic


'''''''''''''''''''''
    personnelInPCopy       含注册土木工程师、安考证、造价工程师、注册化工工程师、造价员、注册建造工程师、注册电气工程师、注册建筑师、注册公用设备工程师、注册结构师
    
    personnelEnterPCopy    含注册土木工程师、安考证、造价工程师、注册化工工程师、造价员、注册结构工程师、注册电气工程师、注册建筑师、注册公用设备工程师、注册建造师

    WCSafetyEngineer       只有安考证
    
    safetyEngineer         只有注册安全工程师
    
    CERegistered           只有造价工程师
        
    WCEngineer             含造价工程师、造价员、注册建造师、安考证
    
'''''''''''''''''''''
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
class ReadPerson(object):
    def __init__(self, cfg):
        self.personDic = {}
        self.cfg = cfg
        self.db = self.cfg.dbPerson
        self.tbCard = self.cfg.tppersonIDCard
        self.loadIDCard()
        
    def log(self): print '--', len(self.personDic), 'Records collect.'
    
    def loadIDCard(self):
        print 'read IDCard information...'
        self.IdCard = {}
        for item in self.tbCard.find():
            item['idCard'] = re.sub('x', 'X', re.sub("\?|？", "", item['personIDCard'].strip()))
            if not P.checkIdCard(item['idCard']): continue
            if item['personId'] not in self.IdCard: self.IdCard[item['idCard']] = item['personId'] 
        print 'read IDCard information OK'    
    
    def _read_personnelInPCopy(self, cursor):
        for item in cursor:
            if 0 == common_process(item, self.personDic):break
        self.log()
    
    def _read_personnelEnterPCopy(self, cursor):
        for item in cursor:
            if 0 == common_process(item, self.personDic):break
        self.log()
         
    def _read_WCSafetyEngineer(self, cursor):
        for item in cursor:
            item['name'] = item['engineerName']
            item['location'] = "四川省"
            item['idCard'] = item['idcard'].strip()
            if P.checkIdCard(item['idCard']) and item['idCard'] in self.IdCard: 
                item['personId'] = self.IdCard[item['idCard']]
            else: 
                item['idCard'] = 'certificateCode' + item['certificateCode']
                item['personId'] = 'idcard-'+ item['idCard'] if item['idCard'] != '' else item['certificateCode']  
            item['companyName'] = item['workUnits']
            if 0 == common_process(item, self.personDic):break
        self.log()
        
    def _read_safetyEngineer(self, cursor):
        common = [['engineerName', 'name'], ['engineerCode', 'certificateCode'], ['workUnits', 'companyName']]        
        for item in cursor:
            for c in common: item[c[1]] = item[c[0]]
            item['location'] = "四川省" if item['companyType'] == "省内企业" else ""
            item['personId'] = 'safe-engineerCode'+ item['engineerCode']
            if 0 == common_process(item, self.personDic):break
        self.log()
            
    def _read_CERegistered(self, cursor):
        common = [['engineerName', 'name'], ['registeredNumb', 'certificateCode'], ['registeredCompany', 'companyName'], ['validDate', 'validityDate'], ['registeredAgencies', 'location']]
        for item in cursor:
            for c in common: item[c[1]] = item[c[0]]  
            item['personId'] = 'ce-registeredNumb'+ item['registeredNumb']
            if item['certificateCode'].find('建[造]')!=0: item['certificateCode'] = item['certificateCode'].replace('建〔造〕', '建[造]')
            if 0 == common_process(item, self.personDic):break
        self.log()
          
    def _read_WCEngineer(self, cursor):
        common = [['qualificationCertNum', 'certificateCode'], ['workingCompany', 'companyName'], ['personType', 'type']]
        lsclr = ['validityDate', 'professional', 'location', 'staffLevel']        
        lsn = { '造价员':1, '造价工程师':1, '质量检测员':1, '建造师':1 }
        for item in cursor:
            for c in common: item[c[1]] = item[c[0]]
            for c in lsclr: item[c] = ''
            if item['type']=='监理工程师' or item['type']=='总监理工程师': continue
            if len(item['gridViewPersonAptitudes'])>0: 
                item['professional'] = item['gridViewPersonAptitudes'][0]['certificateProfessional']
                item['validityDate'] = item['gridViewPersonAptitudes'][0]['expiryDate']
                item['staffLevel'] = item['gridViewPersonAptitudes'][0]['level']
                item['certificateCode'] = item['gridViewPersonAptitudes'][0]['certificateNumber']
            item['idCard'] = item['idCard'].strip()
            if P.checkIdCard(item['idCard']) and item['idCard'] in self.IdCard: 
                item['personId'] = self.IdCard[item['idCard']]
            else:
                item['personId'] = 'idcard-'+ item['idCard']
                if item['idCard']=='': item['personId'] = 'idcard-'+ item['qualificationCertNum']
            if item['type'].encode('utf8') in lsn: item['professional'] = '水利'       
            if 0 == common_process(item, self.personDic):break
        self.log()
    
    def read_personnelInPCopy(self):return self._read_personnelInPCopy(self.db.personnelInPCopy.find())
    def read_personnelEnterPCopy(self):return self._read_personnelEnterPCopy(self.db.personnelEnterPCopy.find())
    def read_WCSafetyEngineer(self):return self._read_WCSafetyEngineer(self.db.WCSafetyEngineer.find())
    def read_safetyEngineer(self):return self._read_safetyEngineer(self.db.safetyEngineer.find())
    def read_CERegistered(self):return self._read_CERegistered(self.db.CERegistered.find())
    def read_WCEngineer(self):return self._read_WCEngineer(self.db.WCEngineer.find())
    
        
'''''''''''''''''''''''''''''''''''''''Class End'''''''''''''''''''''''''''''''''''''''''''''
'''合并同公司同姓名人员'''
def combinePersonByNameAndCompanyname(personDic):
    lsCpPs = {}
    for p in personDic:
        cps = personDic[p]['companyname'].keys()
        if len(cps)==0: cps = {'':1}        
        for cp in cps:
            cpname = cp.encode('utf8')
            personDic[p]['company_name'] = cpname
            if cpname=='': lsCpPs[p] = personDic[p]; continue
            key = cpname + '_'+ personDic[p]['name'].encode('utf8')
            if key not in lsCpPs: 
                lsCpPs[key] = copy.deepcopy(personDic[p]) if len(cps)>1 else personDic[p] 
            else: 
                lsCpPs[key]['certificate'] = dict(personDic[p]['certificate'], **lsCpPs[key]['certificate'])
    print len(lsCpPs), 'combine from', len(personDic)  
    return lsCpPs

'''按姓名合并同公司无PersonId人员'''
def combinePersonNoIdByName(personDic):
    lsPsId = {}
    lsAll = {}
    '''记录有personId的数据'''
    for p in personDic:
        cps = personDic[p]['companyname'].keys()
        if len(cps)==0: cps = {'':1}
        for cp in cps:     
            personDic[p]['company_name'] = cp.encode('utf8')
            key = (cp + '_'+ personDic[p]['name'].strip()).encode('utf8')
            if cp!='' and libP.isPersonId(p):
                if key not in lsPsId: lsPsId[key] = []
                lsPsId[key].append(personDic[p])
    print len(lsPsId)
    '''合并其它来源人员'''
    for p in personDic:
        if libP.isPersonId(p): continue 
        cps = personDic[p]['companyname'].keys()
        if len(cps)==0: cps = {'':1}        
        for cp in cps:                
            personDic[p]['company_name'] = cp.encode('utf8')
            if cp=='': lsAll[p] = personDic[p]; continue
            key = (cp + '_'+ personDic[p]['name'].strip()).encode('utf8')
            if key in lsPsId:
                rindex = int(random.random() * len(lsPsId[key]))
                lsPsId[key][rindex]['certificate'] = dict(personDic[p]['certificate'], **lsPsId[key][rindex]['certificate'])
            else:
                lsAll[p] = personDic[p]
    for key in lsPsId:
        for item in lsPsId[key]: lsAll[item['personId']] = item 
    print len(lsAll), 'combine from', len(personDic)  
    return lsAll


'''写入数据库'''
def writePerson(cfg, personDic):
    lsCompany = P.getCompanyId(cfg.companyInfo)
    index = 50000001
    for p in personDic:
        #cps = personDic[p]['companyname'].keys()
        #cpname = '' if len(cps)==0 else cps[0].encode('utf8')
        cpname = personDic[p]['company_name']
        personDic[p]['companyname'] = cpname
        ls = personDic[p]['certificate'].values()
        personDic[p]['certificate'] = []
        for c in ls: 
            personDic[p]['certificate'].append({'name':c[0], 'professional':c[1], 'level':c[2], 'code':c[3], 'validityDate':c[4]})
        index += 1
        personDic[p]['company_id'] = lsCompany[cpname] if cpname in lsCompany else 0 
        personDic[p]['id'] = index
        personDic[p]['name'] = personDic[p]['name'].replace(' ', '')
        personDic[p]['label'] = 0
        personDic[p]['other'] = ''
        personDic[p]['updateTime'] = datetime.datetime.now()
    print len(personDic)
    cfg.writePerson.insert(personDic.values())
    
def readPerson(cfg): 
    RP = ReadPerson(cfg)
    callbacks = {'personnelInPCopy':RP.read_personnelInPCopy, 'personnelEnterPCopy':RP.read_personnelEnterPCopy,
                         'WCSafetyEngineer':RP.read_WCSafetyEngineer, 'safetyEngineer':RP.read_safetyEngineer,
                         'CERegistered':RP.read_CERegistered, 'WCEngineer':RP.read_WCEngineer
                         }
    for tb in callbacks:
        print 'read and deal table：', tb
        
#        if tb=='personnelInPCopy': continue
        
        '''依次处理各源表'''
        
        callbacks[tb]()
    
    '''''''''''''''''''''人名与公司名同归为一人·该部分可去掉'''''''''''''''''''''''''''''''''
    #personDic = combinePersonByNameAndCompanyname(RP.personDic)
    personDic = combinePersonNoIdByName(RP.personDic)
    ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    return personDic

        
    def __del__(self):  
        for con in self.connect: con.disconnect()
        exit()

if __name__ == '__main__':
    print 'Hello '
    dt = datetime.datetime.now()
    #*********************************************
    _cfg = CFG.Config()
    
#    rp = ReadPerson(_cfg)
        
#    rp.read_personnelInPCopy()
#    rp.read_personnelEnterPCopy()
#    rp.read_safetyEngineer()
#    rp.read_WCSafetyEngineer()
#    rp.read_WCEngineer()
    
#    calcPerson(rp.personDic)
#    exit()

    writePerson(_cfg, readPerson(_cfg))
    _cfg.writePerson.ensure_index('id')
    _cfg.writePerson.ensure_index('company_id')
    
  
    
    #*********************************************
    print datetime.datetime.now(), datetime.datetime.now()-dt
    print 'End !'
