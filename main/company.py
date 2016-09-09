#encoding:utf8
'''
Created on Aug 29, 2016

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

cBase = {'建筑业': ['专业承包', '总承包', '劳务分包', '专项资质', '施工劳务'],
             '工程勘察': ['专业资质', '综合资质', '劳务资质'], 
            '工程设计': ['行业资质', '事务所资质', '专项', '专业资质'], '工程监理': ['专业资质', '综合资质', '事务所资质'],
            '设计施工一体化': ['设计与施工一体化']}
cMaps = {"招标代理":"工程招标代理", "房地产估价":"房地产评估机构", "施工图审图机构":"施工图审图机构", "园林绿化":"城市园林绿化",
        "造价咨询":"造价咨询", "物业服务":"物业服务企业", "房地产开发":"房地产开发企业", "规划编制":"城乡规划编制"}

def findp(ctype, line): return [p for p in cBase[ctype] if line.find(p)!=-1]

#资质处理核心过程
def getLines(item):
    lines = []
    ctype = item['companyBases'][0]['enterpriseType'].encode('utf8')
    for c in item['certificates']:
        if c['qc_code'].find('安许证字')!=-1: continue  
        if c['qc_qualification']==None and c['qc_level']==None and c['qc_code']=="": continue   #无效 
        lv = c['qc_level'].encode('utf8') if c['qc_level'] else ''
        if c['qc_qualification']==None:     #不需要解析专业级别信息
            if lv=='(暂定)' or lv=='暂定级(暂定)' or lv=='': lv = '暂定级'
            row = [ctype, '', '', lv, c['qc_code'], Date_F(c['qc_validityDate'])]
            if ctype in cMaps:    #特定类别及专业 
                row[1] = cMaps[ctype]
                row[2] = row[1]                    
            if row[3].find('、')!=-1: row[3] = row[3].split('、')[0]   #仅一条
            if ctype in { '建筑业':1, '园林绿化':1, '设计施工一体化':1 }: row[0] = '工程施工'
            lines.append(row)
        else:
            stp = c['qc_qualification'].split(',')
            for line in stp:
                stp = re.split('|'.join(cBase[ctype]), line.encode('utf8'))
                p = findp(ctype, line)       #大类，分类，专业，级别, 证书，有效期
                row = [ctype, p[0], '', '', c['qc_code'], Date_F(c['qc_validityDate'])]
                if p==[]: continue
                if len(stp)==3: 
                    if stp[0]=='' and stp[1]=='':
                        if ctype=='工程监理': row[2] = '工程监理综合资质'
                        if ctype=='建筑业' and row[1]=='施工劳务': 
                            row[1] = '劳务'
                            row[2] = '施工劳务'
                        row[3] =  stp[2]
                    else:   #工程设计 专项资质
                        if ctype=='工程设计': 
                            row[1] = findp(ctype, line)[0]+'资质'
                            row[2] = stp[0]+'专项资质'
                            row[3] =  stp[2]
                elif len(stp)==2:
                    if ctype=='建筑业' and len(re.split('公路安全设施|公路机电工程', stp[1]))>1:
                        row[2] = stp[0] + p[0] + stp[1][:-6]
                        row[3] =  stp[1][-6:]
                    elif ctype=='工程监理':
                        row[2] = stp[0]+'监理'
                        row[3] =  stp[1]
                    else:
                        row[2] = stp[0] + p[0]
                        row[3] =  stp[1]
                        if ctype=='工程设计' and stp[1].find('资质')!=-1: row[2] = '环境工程设计' + p[0] #特殊情况
                        if ctype=='工程设计' and row[2].find('专项')!=-1:     #专项特殊情况 
                            row[1]+='资质'
                            row[2]+='资质'
                        if ctype=='工程设计' and p[0]== '专业资质': row[1] = '行业资质'  #专业-》行业
                else:
                    print 'Error', line
                row[3] = re.sub('资质|--请选择--| ', '', row[3])
                if row[3]=='': row[3] = c['qc_level'].encode('utf8')
                if ctype in { '建筑业':1, '园林绿化':1, '设计施工一体化':1 }: row[0] = '工程施工'
                lines.append(row)
                
    return lines
    
def dealCompany():
    con1 = MongoClient('localhost', 27017)
    con2 = MongoClient('192.168.3.45', 27017)
    con3 = MongoClient('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    write = db1.companyInfoNew
    lst = {}    
    
    st = set()
    index = 0
    ls = {}
    
    lsComp = {}
    for item in db2.EInProvenceDetail.find():
        cp = item['companyName']
        ctype = item['companyBases'][0]['enterpriseType'].encode('utf8')
        if ctype=='建设单位': continue
        lines = getLines(item)
        
        if cp not in lsComp:
            line = { 'label':0, 'other':'', 'company_qualification':'', 'companyachievement':[], 
                    'badbehaviors':{"creditScore": 100, "badBehaviorDetails": [] }, 'goodbehaviors':[],  
                    'courtRecords':[], 'bidding':[], 'operationDetail':[],'courtRecords':[], 'honors':[],
                     'certificate':[], 'qualification':{},'company_name':cp, 'updateTime':datetime.datetime.now(), 
                    'company_id':item['entId'], 'companyBases':item['companyBases'][0] }
            line['companyBases']['enterpriseType'] = { ctype:1 }
            lsComp[cp] = line
        for line in lines:
            lmd5 = ','.join(line)
            lsComp[cp]['qualification'][lmd5] = { 'type':line[0], 'class':line[1], 'professional':line[2], 
                                                  'level':line[3], 'code':line[4], 'validityDates':line[5] }
            lsComp[cp]['companyBases']['enterpriseType'][ctype] = 1
            
            pass
    for c in ls:
        if c not in lsComp: print c
    index = 10000001
    dt = []
    for comp in lsComp:
        lsComp[comp]['companyBases']['enterpriseType'] = lsComp[comp]['companyBases']['enterpriseType'].keys()
        lsComp[comp]['company_qualification'] = ','.join([v['professional']+v['level'] for v in lsComp[comp]['qualification'].values()])
        lsComp[comp]['qualification'] = lsComp[comp]['qualification'].values()
        lsComp[comp]['id'] = index
        index += 1
        dt.append(lsComp[comp])
    print len(lsComp)
    write.insert(dt)
    
#        for item in company.find(): #来自公司资质表
#            for k in tpKey: item[k[0]] = k[1]
#            ls[item['company_name']] = item
#            if item['company_name'] in companyDic: 
#                for e in companyDic[item['company_name']]:ls[item['company_name']][e] = companyDic[item['company_name']][e]
#            else:
#                if 'certificate' not in ls[item['company_name']]: ls[item['company_name']]['certificate'] = []
#                if 'companyBases' not in ls[item['company_name']]: ls[item['company_name']]['companyBases'] = {}
#        print len(ls), index
        
#        if len(lines)==0: 
#            for l in item['certificates']:
#                if l['qc_code'].find('安许证字')!=-1: continue
#                if l['qc_qualification']==None and l['qc_level']==None and l['qc_code']=="": continue   #无效 
#                cout(l)
                    
#        lst[type][0].add(list[1])
#        lst[type][1].add(list[2])
#        lst[type][2].add(list[3]) 
            
            
#    for k in lst:
#        print k, ':'
#        print '\t类别：'
#        for e in lst[k][0]: print '\t\t', e
#        print '\t专业：'
#        for e in lst[k][1]: print '\t\t', e
#        print '\t级别：'
#        for e in lst[k][2]: print '\t\t', e 
#        print
    pass
    out(st)
    
def select():
    con1 = MongoClient('localhost', 27017)
    db1 = con1['middle']
    lst = {}
    for item in db1.companyInfoNew.find():
        for q in item['qualification']:
            if q['type'] not in lst: lst[q['type']] = {} 
            if q['class'] not in lst[q['type']]: lst[q['type']][q['class']] = {}
            if q['professional'] not in lst[q['type']][q['class']]: lst[q['type']][q['class']][q['professional']] = {}
            if q['level'] not in lst[q['type']][q['class']][q['professional']]: lst[q['type']][q['class']][q['professional']][q['level']] = 0
            lst[q['type']][q['class']][q['professional']][q['level']] += 1
            pass
        
    for t in lst:
        print t, ':'
        for c in lst[t]:
            print '\t', c, ':'
            for p in lst[t][c]:
                print '\t\t', p, ':',
                for l in lst[t][c][p]:
                    print l, '', lst[t][c][p][l], ',',
                print   
            
  #拍自己一下
  #我要把这些写到代码注释里！
  #写啊  
  #然后提交到github
  #反正也没人看到
  #我的这些代码会定期执行，很多人会可能看
  #那你提交个我看看，我要提交……
  #不让你提交
  #我给领导说，现在电脑被黑客入侵了，没法提交
  
def Date_F(str):
#    str = str.decode('utf8')
    if not str or str == '': return ''
    if not (str.find("年")>0 and str.find("月")>0): return str
    sp = re.split("年|月".decode('utf8'), str)
    return (sp[0] + '-' + sp[1] + '-' + sp[2].replace('日', '')).encode('utf8')

if __name__ == '__main__':
    print 'Hello '
    dt = datetime.datetime.now()
    #*********************************************
#    dealCompany()
    
#    select()
    
    #*********************************************
    print datetime.datetime.now(), datetime.datetime.now()-dt
    print 'End !'