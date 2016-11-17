#coding:utf8
from pymongo import MongoClient

class Config(object):
    def __init__(self):
        con1 = MongoClient('192.168.3.119', 27017)
        con2 = MongoClient('192.168.3.45', 27017)
        con3 = MongoClient('101.204.243.241', 27017)
        con4 = MongoClient('192.168.3.221', 27017)
        db1 = con1['middle']
        db2 = con2['constructionDB']
        db3 = con3['jianzhu3']
        db4 = con4['jianzhu3']
        db5 = con1['jianzhu']
        self.connect = [con1, con2, con3, con4]
        self.tbProvenceIn = db2.EInProvenceDetail
        self.tbProvenceOut = db2.EOutProvenceDetail
        self.companyInfo = db1.companyInfoNew
        self.writeCompany = db1.companyInfoNew
        self.writePerson = db1.personNew
        self.writeBidding = db1.bidding
        self.dbPerson = db2
        self.dbCompany = db2
        self.dbSource = db2
        self.dbBidding = db2
        self.dbBidding2 = db5
        self.tbBidding = db1.bidding
        self.companyAchievement = db2.companyAchievement
        self.dbNow = db4
        self.tbsIndex = [{'company':db4.companyInfoNew, 'person':db4.personNew, 'bidding':db4.bidding},
                         {'company':db3.companyInfoNew, 'person':db3.personNew, 'bidding':db3.bidding}]
        self.tbsSpecial = [db4.SpecialCondition, db3.SpecialCondition]
        
    def __del__(self):  
        for con in self.connect: con.disconnect()
        
        
 