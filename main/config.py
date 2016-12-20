#coding:utf8
from pymongo import MongoClient
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class Config(object):
    def __init__(self):
        #con1 = MongoClient('101.204.243.241', 27017)
        con1 = MongoClient('10.101.1.119', 27017)       #本地库      
        #con2 = MongoClient('101.204.243.241', 27017)    #远程库
        con2 = MongoClient('10.101.1.224', 27017)        #数据源
        db1 = con1['jianzhu']
        #db1 = con1['middle']
        db2 = con2['constructionDB']
        #db3 = con2['jianzhu3']
        db3 = con1['jianzhu']
        #db1.authenticate("readWriteAny","abc@123","admin")
        #db2.authenticate("readWriteAny","abc@123","admin")
        #db3.authenticate("readWriteAny","abc@123","admin")
        self.connect = [con1, con2]
        self.tbProvenceIn = db2.EInProvenceDetail
        self.tbProvenceOut = db2.EOutProvenceDetail
        self.companyInfo = db1.companyInfoNew
        self.writeCompany = db1.companyInfoNew
        self.writePerson = db1.personNew
        self.writeBidding = db1.bidding
        self.midCompany = db3.companyInfoNew
        self.midPerson = db3.personNew
        self.midBidding = db3.bidding
        self.dbPerson = db2
        self.dbCompany = db2
        self.dbSource = db2
        self.dbBidding = db2
        self.tbBidding = db1.bidding
        self.companyAchievement = db2.companyAchievement
        self.dbNow = db1
        self.tbLog = db1.myLog
        self.tbsIndex = [{'company':db1.companyInfoNew, 'person':db1.personNew, 'bidding':db1.bidding},
                         {'company':db3.companyInfoNew, 'person':db3.personNew, 'bidding':db3.bidding}]
        self.tbsSpecial = [db3.SpecialCondition, db3.SpecialCondition]

#        con1 = MongoClient('192.168.3.119', 27017)
#        con2 = MongoClient('192.168.3.45', 27017)
#        con3 = MongoClient('101.204.243.241', 27017)
#        con4 = MongoClient('192.168.3.221', 27017)
#        db1 = con1['middle']
#        db2 = con2['constructionDB']
#        db3 = con3['jianzhu3']
#        db4 = con4['jianzhu3']
#        db5 = con1['jianzhu']
#        self.connect = [con1, con2, con3, con4]
#        self.tbProvenceIn = db2.EInProvenceDetail
#        self.tbProvenceOut = db2.EOutProvenceDetail
#        self.companyInfo = db1.companyInfoNew
#        self.writeCompany = db1.companyInfoNew
#        self.writePerson = db1.personNew
#        self.writeBidding = db1.bidding
#        self.dbPerson = db2
#        self.dbCompany = db2
#        self.dbSource = db2
#        self.dbBidding = db2
#        self.dbBidding2 = db5
#        self.tbBidding = db1.bidding
#        self.companyAchievement = db2.companyAchievement
#        self.dbNow = db4
#        self.tbsIndex = [{'company':db4.companyInfoNew, 'person':db4.personNew, 'bidding':db4.bidding},
#                         {'company':db3.companyInfoNew, 'person':db3.personNew, 'bidding':db3.bidding}]
#        self.tbsSpecial = [db4.SpecialCondition, db3.SpecialCondition]
        
        
        
    def __del__(self):  
        pass
        #for con in self.connect: con.disconnect()
        
        
 