'''
Created on Oct 9, 2016

@author: Administrator
'''
from pymongo import MongoClient
import datetime
import createCompany as CC
import createPerson as CP
import createBidding as CB
import createIndex as CL
import config as CFG
import createUpdate as CU
import sys
reload(sys)
sys.setdefaultencoding('utf-8')      

if __name__ == '__main__':
    print 'start all deal program '
    dt = datetime.datetime.now()
    #*********************************************
    _cfg = CFG.Config()
    
    print 'create table ', _cfg.writeCompany.name
    CC.writeCompanyIn(_cfg, CC.readCompanyIn(_cfg))
    CC.writeCompanyOut(_cfg, CC.readCompanyOut(_cfg))
    
    _cfg.writeCompany.create_index('id')
    
    #exit()
    print 'create table ', _cfg.writePerson.name
    CP.writePerson(_cfg, CP.readPerson(_cfg))
    
    print 'create table ', _cfg.writeBidding.name
    CB.writeBidding(_cfg, CB.readBidding(_cfg))
    CB.addBiddingFromcompanyAchievement(_cfg)
    CB.addCompanyBiddingCount(_cfg)
    
    print 'update table information', _cfg.writePerson.name
    CU.updateCompanyBase(_cfg)
    CU.updateGoodRecord(_cfg)
    CU.updateHonors(_cfg)
    CU.updateNewCourt(_cfg)
    
    print 'create indexs'
    CL.createIndexs(_cfg)
    CL.listIndexs(_cfg)
    
#   CL.addIdForSpecialCompany(_cfg)

    
    #*********************************************
    print datetime.datetime.now(), datetime.datetime.now()-dt
    print 'End !'
    
    