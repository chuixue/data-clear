# -*- coding:utf-8 -*-
# encoding: utf-8
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
sys.setdefaultencoding( "utf-8" )
import traceback

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

    def IS_AnKaoZhengi(self, item):
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

    # 把记录写入搜索引擎
    def ReadMongoTable(self):
#        connection = pymongo.Connection('192.168.3.45', 27017)
        
        connection = MongoClient('192.168.3.45', 27017)
        db = connection.constructionDB

        company_table = db.ElnPQualification
        jianzaoshi_table = db.personnellnPCopy
        jianzaoshi2_table = db.personnelEnterPCopy

        ankaozheng_table = db.WCSafetyEngineer

        anquangongchengshi_table = db.safetyEngineer

        zaojiashi_table = db.CERegistered
        ES_Dict = {}
        
        
        # 写建设厅、建造师数据
        for item in jianzaoshi_table.find():
            print item
            
            self.AddItem2Dict(item,ES_Dict,'name', 'name');
            self.AddItem2Dict(item,ES_Dict,'location', 'location');
            self.AddItem2Dict(item,ES_Dict,'companyName', 'companyname');
            self.AddItem2Dict(item,ES_Dict,'name', 'name');
            self.AddItem2Dict(item, ES_Dict, 'post', 'workpost');
            if self.IS_JianZaoShi(item):
                #AddIetm2Dict(item,Dict,ItemKey,DictKey);
                self.AddItem2Dict(item, ES_Dict, 'certificateCode', 'jz_Code');
                self.AddItem2Dict(item, ES_Dict, 'validityDate', 'jz_validityDate');
                self.AddItem2Dict(item, ES_Dict, 'staffLevel', 'jz_staffLevel');
                self.AddItem2Dict(item, ES_Dict, 'type', 'jz_type');

            elif self.IS_AnKaoZheng(item):
                self.AddItem2Dict(item, ES_Dict, 'certificateCode', 'ak_Code');
                self.AddItem2Dict(item, ES_Dict, 'validityDate', 'ak_validityDate');

            elif self.IS_ZaoJiaGongChengShi(item):
                self.AddItem2Dict(item, ES_Dict, 'certificateCode', 'zj_Code');
                self.AddItem2Dict(item, ES_Dict, 'validityDate', 'zj_validityDate');
                self.AddItem2Dict(item, ES_Dict, 'staffLevel', 'zj_staffLevel');
                self.AddItem2Dict(item, ES_Dict, 'type', 'zj_type');

            elif self.IS_ZaoJiaYuan(item):
                self.AddItem2Dict(item, ES_Dict, 'certificateCode', 'zjy_Code');
                self.AddItem2Dict(item, ES_Dict, 'validityDate', 'zjy_validityDate');
                self.AddItem2Dict(item, ES_Dict, 'staffLevel', 'zjy_staffLevel');
                self.AddItem2Dict(item, ES_Dict, 'type', 'zjy_type');
                

        for item in jianzaoshi2_table.find():
            print item

        # 写安考证
        for item in ankaozheng_table.find():
            print item
            self.AddItem2Dict(item, ES_Dict, 'engineerName', 'name');
            self.AddItem2Dict(item, ES_Dict, 'engineerGender', 'gender');
            self.AddItem2Dict(item, ES_Dict, 'engineerTitle', 'title');
            self.AddItem2Dict(item, ES_Dict, 'idcard', 'idcard');

            self.AddItem2Dict(item, ES_Dict, 'workUnits', 'companyname');
            self.AddItem2Dict(item, ES_Dict, 'certificateCode', 'ak_code');
            self.AddItem2Dict(item, ES_Dict, 'certificateStatus', 'ak_status');
            self.AddItem2Dict(item, ES_Dict, 'validityDate', 'ak_validityDate');
            self.AddItem2Dict(item, ES_Dict, 'companyType', 'ak_companytype');

        #写安全工程师
        for item in anquangongchengshi_table.find():
            print item
            self.AddItem2Dict(item, ES_Dict, 'engineerName', 'name');
            self.AddItem2Dict(item, ES_Dict, 'engineerGender', 'gender');
            self.AddItem2Dict(item, ES_Dict, 'workUnits', 'companyname');

            self.AddItem2Dict(item, ES_Dict, ' companyType', 'aq_Code');
            self.AddItem2Dict(item, ES_Dict, 'registeredType', 'aq_registeredtype');
            self.AddItem2Dict(item, ES_Dict, ' validityDate', 'aq_validitydate');
            self.AddItem2Dict(item, ES_Dict, 'engineerCode', 'aq_code');


#写造价工程师
        for item in jianzaoshi_table.find():
            print item
            self.AddItem2Dict(item, ES_Dict, 'engineerName', 'name');
            self.AddItem2Dict(item, ES_Dict, 'engineerGender', 'gender');
            self.AddItem2Dict(item, ES_Dict, 'registeredCompany', 'companyname');
            self.AddItem2Dict(item, ES_Dict, 'branchCompany', 'branchcompany');
            self.AddItem2Dict(item, ES_Dict, 'registeredAgencies', 'zj_registeredagencies');

            self.AddItem2Dict(item, ES_Dict, ' companyType', 'zj_companytype');
            self.AddItem2Dict(item, ES_Dict, 'registeredType', 'zj_registeredtype');
            self.AddItem2Dict(item, ES_Dict, ' validityDate', 'zj_validitydate');
            self.AddItem2Dict(item, ES_Dict, 'status', 'zj_code');
            self.AddItem2Dict(item, ES_Dict, 'registeredNumb', 'zj_code');

        #写公司资质
        for item in  company_table.find():
            print item
            self.AddItem2Dict(item, ES_Dict, 'qualificationType', 'company_qualificationType');
            self.AddItem2Dict(item, ES_Dict, 'professionalType', 'company_professionalType');
            self.AddItem2Dict(item, ES_Dict, 'professionalLevel', 'company_professionalLevel');

        #写公司（这里会出现重名BUG，但不管啦）
        res = self.es.index(index="jh-index", doc_type='jh-type',id=(ES_Dict["companyname"] + "#" + ES_Dict["name"]), body=ES_Dict)

    #搜索
        #搜索公司DICT拼写
        #搜索公司DICT组合
        #搜索人员DICT拼写
        #搜索人员DICT组合
        #搜索

        pass
    
    
    
    
    
    m = {'jz':[['certificateCode', 'jz_Code'], ['validityDate', 'jz_validityDate'],['staffLevel', 'jz_staffLevel'], ['type', 'jz_type']],
                  'ak':[['certificateCode', 'ak_Code'], ['validityDate', 'ak_validityDate']],
                  'zj':[['certificateCode', 'zj_Code'], ['validityDate', 'zj_validityDate'],['staffLevel', 'zj_staffLevel'], ['type', 'zj_type']],
                  'zjy':[['certificateCode', 'zjy_Code'], ['validityDate', 'zjy_validityDate'],['staffLevel', 'zjy_staffLevel'], ['type', 'zjy_type']]
            }
    
    
    print item['name'], item['companyName']
                print personDic[personId]['name'],
                cout(personDic[personId]['companyname'])
    
    
    
    