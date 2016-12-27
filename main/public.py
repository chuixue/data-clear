#coding:utf8
'''
Created on Sep 9, 2016

@author: Administrator
'''
import datetime
import re
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def cout(ls):
    for l in ls: print l, ':', ls[l] if type(ls[l])!=type('') else ls[l].encode('utf8'),
    print
def out(ls):
    for l in ls: print l,
    print




def checkIdCard(id_number):
    if len(id_number) != 18 and len(id_number) != 15: return False
    if len(id_number) == 18 and not re.match(r"^\d{17}(\d|X|x)$", id_number): return False
    if len(id_number) == 15 and not re.match(r"\d+$", id_number): return False
    
    
    return True

def _checkIdCard(id_number):  
    area_dict = {11:1,12:1,13:1,14:1,15:1,21:1,22:1,23:1,31:1,32:1,33:1,34:1,35:1,36:1,37:1,41:1,42:1,43:1,44:1,45:1,46:1,50:1,51:1,52:1,53:1,54:1,61:1,62:1,63:1,64:1,71:1,81:1,82:1,91:1}
    '''
    area_dict = {11: "北京", 12: "天津", 13: "河北", 14: "山西", 15: "内蒙古", 21: "辽宁", 22: "吉林", 23: "黑龙江", 31: "上海", 32: "江苏",  
                 33: "浙江", 34: "安徽", 35: "福建", 36: "江西", 37: "山东", 41: "河南", 42: "湖北", 43: "湖南", 44: "广东", 45: "广西",  
                 46: "海南", 50: "重庆", 51: "四川", 52: "贵州", 53: "云南", 54: "西藏", 61: "陕西", 62: "甘肃", 63: "青海", 64: "新疆",  
                 71: "台湾", 81: "香港", 82: "澳门", 91: "外国"}
    '''  
    id_code_list = [7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]  
    check_code_list = [1, 0, 'X', 9, 8, 7, 6, 5, 4, 3, 2]  
    if len(id_number) != 18:  
        return False  
    if not re.match(r"^\d{17}(\d|X|x)$", id_number):  
        return False  
    if int(id_number[0:2]) not in area_dict:  
        return False 
    try:  
        datetime.date(int(id_number[6:10]), int(id_number[10:12]), int(id_number[12:14]))  
    except ValueError:  
        return False  
    if str(check_code_list[sum([a * b for a, b in zip(id_code_list, [int(a) for a in id_number[0:-1]])]) % 11]) != id_number.upper()[-1]:  
        return False
    return True  


def dbKeys(table, keys):
    temp = dict((k, 1) for k in keys)
    lskey = {}
    for item in table.find({}).limit(1): lskey = dict((key, 0) for key in item if key not in temp)
    return lskey

def getCompanyId(table):
    return dict((item['company_name'].encode('utf8'), 
                 item['id']) for item in table.find({}, dbKeys(table, ['company_name', 'id']))) 

def getMaxId(table, name):
    if len(dbKeys(table, [name]))==0: return -1 
    return table.find({}, dbKeys(table, [name])).sort(name, -1).limit(1)[0][name]

def strToDate(_str):
    try:
        sp = _str.strip().split('-')
        dt = datetime.datetime(int(sp[0]), int(sp[1]), int(sp[2]))
        return dt
    except:
        return None
 
def dateToStr(_date):
    if not _date: return ''
    return _date.strftime('%Y-%m-%d').encode('utf8')

def dateFormat(_str):
    return dateToStr(strToDate(_str)) 

def haveNum(_s):
    if (not _s) or _s.strip()=='': return False
    st = set(_s)
    num = dict([[str(i), 1] for i in range(0,10)])
    for s in st:
        if s in num: return True
    return False

def Date_F(str):
    if not str or str == '': return ''
    sp = []
    if str.find("年")>0 and str.find("月")>0: 
        sp = re.split("年|月".decode('utf8'), str)
    elif str.find("-")>0:
        sp = re.split("-".decode('utf8'), str)
    if len(sp)!=3: return str
    try:
        _dt = datetime.datetime(int(sp[0]), int(sp[1]), int(sp[2].replace('日', '')))
        _dts = _dt.strftime('%Y-%m-%d').encode('utf8')
        return _dts
    except: return ''
