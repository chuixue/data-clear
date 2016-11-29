'''
Created on Oct 9, 2016

@author: Administrator
'''
from pymongo import MongoClient
import public as P
import config as CFG
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def createIndexs(cfg):
    for db in cfg.tbsIndex:
        db['company'].create_index('id', unique=True)
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
    lsCompany = P.getCompanyId(cfg.dbNow.companyInfoNew)
    
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
 
    
if __name__ == '__main__':
    cfg = CFG.Config()
    
    createIndexs(cfg)
#    listIndexs(cfg)
#    addIdForSpecialCompany(cfg)
    
    