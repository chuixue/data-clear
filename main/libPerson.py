#coding:utf8
'''
Created on Sep 19, 2016

@author: Administrator
'''

import datetime
import re

def Date_F(str):
    str = str.decode('utf8')
    if not str or str == '': return ''
    if not (str.find("年")>0 and str.find("月")>0): return str.encode('utf8')
    sp = re.split("年|月".decode('utf8'), str)
    return (sp[0] + '-' + sp[1] + '-' + sp[2].replace('日', '')).encode('utf8')


class Type(object):
    def __init__(self):self.index = 0
    
    @classmethod
    def getType(self, _item):
        tp = Type()
        if tp.IS_JianZaoShi(_item):return 'jz'
        elif tp.IS_AnKaoZheng(_item):return 'ak'
        elif tp.IS_ZaoJiaGongChengShi(_item):return 'zj'
        elif tp.IS_ZaoJiaYuan(_item):return 'zjy'        
        elif tp.IS_ZhuCeAnQuan(_item):return 'aq' 
        elif tp.IS_JIANZHUSHI(_item):return 'jzs'
        elif tp.IS_JIANLI(_item):return 'jl'
        elif tp.IS_KANCHA(_item):return 'kc'
        elif tp.IS_JIEGOU(_item):return 'jg'
        elif tp.IS_ZYJS(_item):return 'zyjs'
        elif tp.IS_OTHER(_item):return 'other'
        
        
    def IS_JianZaoShi(self, item):   #注册建造师
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

def dealType(type, item):
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

