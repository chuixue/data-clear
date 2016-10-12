#coding:utf8
'''
Created on Sep 19, 2016

@author: Administrator
'''
import re
import datetime
import public as P

def cout(ls):
    for l in ls: print l, ':', ls[l],
    print
def out(ls):
    for l in ls: print l,
    print

cBase = {'建筑业': ['专业承包', '总承包', '劳务分包', '专项资质', '施工劳务'],
             '工程勘察': ['专业资质', '综合资质', '劳务资质'], 
            '工程设计': ['综合资质', '行业资质', '事务所资质', '专项', '专业资质'], '工程监理': ['专业资质', '综合资质', '事务所资质'],
            '设计施工一体化': ['设计与施工一体化'],
            "招标代理": ["工程招标代理机构"], '造价咨询':['所有序列'], '物业服务':['所有序列'],
            '园林绿化':['所有序列'], '房地产估价':['资质'] }

cMaps = {"招标代理":"工程招标代理", "房地产估价":"房地产评估机构", "施工图审图机构":"施工图审图机构", "园林绿化":"城市园林绿化",
        "造价咨询":"造价咨询", "物业服务":"物业服务企业", "房地产开发":"房地产开发企业", "规划编制":"城乡规划编制"}

cProfessionals = {
                 "电信工程专业承包":"电子与智能化工程专业承包", 
                "电子工程专业承包":"电子与智能化工程专业承包", 
                "送变电工程专业承包":"输变电工程专业承包", 
                "园林古建筑工程专业承包":"古建筑工程专业承包", 
                "公路交通工程公路机电工程专业承包":"公路交通工程专业承包公路机电工程", 
                "公路交通工程公路安全设施专业承包":"公路交通工程专业承包公路安全设施", 
                "机电设备安装工程专业承包":"建筑机电安装工程专业承包", 
                "预拌商品混凝土专业承包":"预拌混凝土专业承包", 
                "特种专业工程专业承包":"特种工程专业承包", 
                "防腐保温工程专业承包":"防水防腐保温工程专业承包", 
                "建筑智能化工程专业承包":"电子与智能化工程专业承包", 
                "建筑防水工程专业承包":"防水防腐保温工程专业承包", 
                "混凝土预制构件专业承包":"预拌混凝土专业承包", 
                "房屋建筑工程施工总承包":"建筑工程施工总承包",  
                "冶炼工程施工总承包":"冶金工程施工总承包", 
                "化工石油工程施工总承包":"石油化工工程施工总承包",
                "地基与基础工程专业承包" : "地基基础工程专业承包", 
                "地基基础专业承包" : "地基基础工程专业承包", 
                "机电安装工程施工总承包" : "机电工程施工总承包"
            }
cProfessionalsIn = { 
                 "地基与基础工程专业承包" : "地基基础工程专业承包", 
                 "地基基础专业承包" : "地基基础工程专业承包", 
                 "机电安装工程施工总承包" : "机电工程施工总承包"
            }

def findp(ctype, line): return [p for p in cBase[ctype] if line.find(p)!=-1]

def initLine(item):
    line = { 'label':0, 'other':'', 'company_qualification':'', 'companyachievement':[], 
                    'badbehaviors':{"creditScore": 100, "badBehaviorDetails": [] }, 'goodbehaviors':[],  
                    'courtRecords':[], 'biddingCount':0, 'operationDetail':[],'courtRecords':[], 'honors':[],
                     'certificate':[], 'qualification':{}, 'updateTime':datetime.datetime.now(), 
                    'company_id':item['entId'], 'companyBases':item['companyBases'][0], 'company_name':item['companyName'] }
    return line

#处理入川情况
def getLinesOut(item):
    lines = []
    ctype = item['companyBases'][0]['enterpriseType'].encode('utf8')
    ctype = re.sub('入川', '', ctype)
    for c in item['certificates']:
        if c['qc_qualification']==None and c['qc_level']==None and c['qc_code']=="": continue   #无效 
        lv = c['qc_level'].encode('utf8') if c['qc_level'] else ''
        if c['qc_qualification']==None or c['qc_qualification'].strip()=='':     #不需要解析专业级别信息
            if lv=='(暂定)' or lv=='暂定级(暂定)' or lv=='': lv = '暂定级'
            row = [ctype, '', '', lv, c['qc_code'], P.Date_F(c['qc_validityDate'])]
            if ctype in cMaps:    #特定类别及专业 
                row[2] = row[1] = cMaps[ctype]         
            if row[3].find('、')!=-1: row[3] = row[3].split('、')[0]   #仅一条
            if ctype in { '建筑业':1, '园林绿化':1, '设计施工一体化':1 }: row[0] = '工程施工'
            lines.append(row)
        else:
            _stp = c['qc_qualification'].encode('utf8').split(',')
            stp = []
            for line in _stp:
                if line.count('级')>1:
                    for s in (k+'级' for k in line.split('级') if k!=''): stp.append(s)
                else: stp.append(line)
            for line in stp:
                line = line.encode('utf8').replace('(旧)', '').strip()
                if line=='': continue
                if ctype=='工程设计' and line.find('工程勘察')!=-1 and line.find('设计')==-1: ctype='工程勘察'
                if ctype=='工程勘察' and line.find('工程设计')!=-1 and line.find('勘察')==-1: ctype='工程设计'
                stp = re.split('|'.join(cBase[ctype]), line.encode('utf8'))
                p = findp(ctype, line)       #大类，分类，专业，级别, 证书，有效期
                if ctype=='工程监理' and len(p)==0: p=['专业资质']
                row = [ctype, p[0], '', '', c['qc_code'], P.Date_F(c['qc_validityDate'])]
                if ctype in cMaps:  #制定类别及专业
                    if len(stp)==2:
                        row[1] = row[2] = cMaps[ctype]
                    elif len(stp)==3:
                        if p[0]=='工程招标代理机构': row[1] = row[2] = '工程招标代理'                            
                    row[3] = stp[-1]
                else:
                    if len(stp)==3:
                        if stp[0]=='' and stp[1]=='': 
                            if ctype=='工程监理': 
                                row[2] = '工程监理综合资质'
                            if ctype=='建筑业' and row[1]=='施工劳务':
                                row[1] = '劳务'; row[2] = '施工劳务'
                            row[3] =  stp[2]
                        else:   #工程设计 专项资质
                            if ctype=='工程设计':
                                row[1] = findp(ctype, line)[0]
                                row[2] = stp[0]+'专项资质'
                                row[3] =  stp[2]
                                if row[1]=='专项': row[1]+='资质'
                                if stp[0]=='工程设计': 
                                    row[1] = '综合资质'; row[2] = '工程设计综合资质'
                    elif len(stp)==2:
                        if ctype=='建筑业' and len(re.split('公路安全设施|公路机电工程', stp[1]))>1:
                            row[2] = stp[0] + p[0] + stp[1][:-6]
                            row[3] =  stp[1][-6:]
                        elif ctype=='工程监理':
                            row[2] = '工程监理综合资质' if stp[0]=='' else stp[0]+'监理' 
                            row[3] =  stp[1]
                        else:
                            row[3] =  stp[1]
                            if ctype=='建筑业' and row[3].find('级')==-1 or row[3].find('一级及以下公路')!=-1:
                                row[3] =  '暂定级'
                                if stp[0].find('特种')!=-1: stp[0] = '特种工程'
                                if stp[0].find('公路交通工程')!=-1:
                                    stp[0] += '公路安全设施' if stp[1].find('安全')!=-1 else '公路机电工程'
                            if ctype=='建筑业' and stp[0].find('特种')!=-1 and row[3].find('(')!=-1: row[3]='不分等级' 
                            row[2] = stp[0] + p[0]
                            if ctype=='工程设计' and stp[1].find('资质')!=-1: row[2] = '环境工程设计' + p[0] #特殊情况
                            if ctype=='工程设计' and row[2].find('专项')!=-1:     #专项特殊情况 
                                row[1]+='资质'; row[2]+='资质'
                    elif len(stp)==1:
                        if ctype=='工程监理':
                            stp = re.split('工程监理', line.encode('utf8').replace(' ', ''))
                            try:
                                row[2] = stp[0] + '工程监理'; row[3] = stp[1]
                            except:
                                print c['qc_qualification']
                    row[3] = re.sub('资质|--请选择--| ', '', row[3])
                    if row[3]=='': row[3] = c['qc_level'].encode('utf8')
                    if ctype in { '建筑业':1, '园林绿化':1, '设计施工一体化':1 }: row[0] = '工程施工'
                    if ctype=='工程设计' and p[0]== '专业资质': row[1] = '行业资质'  #专业-》行业
                    lines.append(row) 
    return lines

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
            row = [ctype, '', '', lv, c['qc_code'], P.Date_F(c['qc_validityDate'])]
            if ctype in cMaps:    #特定类别及专业 
                row[2] = row[1] = cMaps[ctype]                 
            if row[3].find('、')!=-1: row[3] = row[3].split('、')[0]   #仅一条
            if ctype in { '建筑业':1, '园林绿化':1, '设计施工一体化':1 }: row[0] = '工程施工'
            lines.append(row)
        else:
            stp = c['qc_qualification'].split(',')
            for line in stp:
                stp = re.split('|'.join(cBase[ctype]), line.encode('utf8'))
                p = findp(ctype, line)       #大类，分类，专业，级别, 证书，有效期
                if p==[] or line.strip()=='': continue
                row = [ctype, p[0], '', '', c['qc_code'], P.Date_F(c['qc_validityDate'])]
                if len(stp)==3: 
                    if stp[0]=='' and stp[1]=='':
                        if ctype=='工程监理': row[2] = '工程监理综合资质'
                        if ctype=='建筑业' and row[1]=='施工劳务': 
                            row[1] = '劳务'; row[2] = '施工劳务'
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
                            row[1]+='资质'; row[2]+='资质'
                        if ctype=='工程设计' and p[0]== '专业资质': row[1] = '行业资质'  #专业-》行业
                else:
                    print 'Error', line
                row[3] = re.sub('资质|--请选择--| ', '', row[3])
                if row[3]=='': row[3] = c['qc_level'].encode('utf8')
                if ctype in { '建筑业':1, '园林绿化':1, '设计施工一体化':1 }: row[0] = '工程施工'
                lines.append(row)
                
    return lines