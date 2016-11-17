#coding:utf8
'''
Created on Oct 21, 2016

@author: Administrator
'''
import sys 
sys.path.append('D:/ftp') 

from pymongo import MongoClient


class Config(object):
    def __init__(self):
        con1 = MongoClient('localhost', 27017)
        db1 = con1['jianzhu3']
        self.companyInfo = db1.companyInfoNew
        self.dbNow = db1
        self.tbsIndex = [{'company':db1.companyInfoNew, 'person':db1.personNew, 'bidding':db1.bidding}]
        self.tbsSpecial = [db1.SpecialCondition]
        
    def __del__(self):  
        pass
        #for con in self.connect: con.disconnect()
        
        
def createIndexs(cfg):
    for db in cfg.tbsIndex:
        db['company'].create_index('id')
        db['company'].create_index('company_id')
        
        db['person'].create_index('id')
        db['person'].create_index('company_id')
        db['person'].create_index('personId')
        
        db['bidding'].create_index('id')
        db['bidding'].create_index('updateTime')
        db['bidding'].create_index('company_id')
        db['bidding'].create_index([("architects", 1), ("id", -1)])

def listIndexs(cfg):
    for db in cfg.tbsIndex:
        print 'deal table ', db['company'].name
        print db['company'].name
        for index in db['company'].list_indexes():print '--', index
        print db['person'].name
        for index in db['person'].list_indexes():print '--', index
        print db['bidding'].name
        for index in db['bidding'].list_indexes():print '--', index

def addIdForSpecialCompany(cfg):
    lsUpdate = []
    lsCompany = getCompanyId(cfg.companyInfo)
    
    for tb in cfg.tbsSpecial:
        print 'deal table ', tb.name
        index = 0
        for item in tb.find():
            cpname = item['company_name'].encode('utf8')
            id = lsCompany[cpname] if cpname in lsCompany else 0 
            lsUpdate.append([{'company_name': item['company_name']}, {'$set':{'company_id':id}}])
        print 'update all the data,', len(lsUpdate)
        for b in lsUpdate: 
            tb.update(b[0], b[1])
            index += 1
            if index % 2000 == 0: print index
        print 'OK!'         

#---------------------------------------------------------------------- 
def dbKeys(table, keys):
    temp = dict((k, 1) for k in keys)
    for item in table.find({}).limit(1): lskey = dict((key, 0) for key in item if key not in temp)
    return lskey

def getCompanyId(table):
    return dict((item['company_name'].encode('utf8'), 
                 item['id']) for item in table.find({}, dbKeys(table, ['company_name', 'id']))) 
#----------------------------------------------------------------------

   
   
if __name__ == '__main__':
    cfg = Config()
    
    createIndexs(cfg)
    listIndexs(cfg)
    addIdForSpecialCompany(cfg)
    