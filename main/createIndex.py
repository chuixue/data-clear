'''
Created on Oct 9, 2016

@author: Administrator
'''
from pymongo import MongoClient

class Config(object):
    def __init__(self):
        con1 = MongoClient('localhost', 27017)
        con2 = MongoClient('192.168.3.45', 27017)
        con3 = MongoClient('101.204.243.241', 27017)
        con4 = MongoClient('192.168.3.221', 27017)
        db1 = con1['middle']
        db2 = con2['constructionDB']
        db3 = con3['jianzhu3']
        db4 = con4['jianzhu3']
        self.writeCompany = db1.companyInfoNew
        self.writePerson = db1.personNew
        self.writeBidding = db1.bidding

def createIndexs(cfg):
    cfg.writeCompany.create_index('id')
    cfg.writeCompany.create_index('company_id')
    
    cfg.writePerson.create_index('id')
    cfg.writePerson.create_index('company_id')
    cfg.writePerson.create_index('personId')
    
    cfg.writeBidding.create_index('id')
    cfg.writeBidding.create_index('updateTime')
    cfg.writeBidding.create_index('company_id')
    cfg.writeBidding.create_index([("architects", 1), ("id", -1)])

def listIndexs(_cfg):
    print _cfg.writeCompany.name
    for index in _cfg.writeCompany.list_indexes():print '--', index
    print _cfg.writePerson.name
    for index in _cfg.writePerson.list_indexes():print '--', index
    print _cfg.writeBidding.name
    for index in _cfg.writeBidding.list_indexes():print '--', index
    
if __name__ == '__main__':
    cfg = Config()
    
    createIndexs(cfg)
    listIndexs(cfg)
    
    
    