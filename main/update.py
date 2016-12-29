#coding: utf8
'''
Created on Dec 29, 2016

@author: Administrator
'''
import datetime
import libLog as LG
import config as CFG
import createUpdate as CU
import updateCompany as UC
import updatePerson as UP
import updateBidding as UB
import createBidding as CB

import sys
reload(sys)
sys.setdefaultencoding('utf-8')      

if __name__ == '__main__':
    print 'start all update program '
    dt = datetime.datetime.now()
    lg = LG.Log()
    _cfg = CFG.Config()
    #*********************************************
    
    lg.log('update table ', _cfg.writeCompany.name)
    UC.updateCompanyIn(_cfg)
    UC.updateCompanyOut(_cfg)
    
    
    lg.log('update table ', _cfg.writePerson.name)
    UP.updatePerson(_cfg, UP.readPerson(_cfg))
    
    
    lg.log('update table ', _cfg.writeBidding.name)
    UB.updateBidding(_cfg, UB.readBidding(_cfg))
    CB.addBiddingFromcompanyAchievement(_cfg)
    CB.addCompanyBiddingCount(_cfg)
    
    
    lg.log('update table information', _cfg.writePerson.name)
    CU.updateCompanyBase(_cfg)
    CU.updateGoodRecord(_cfg)
    CU.updateHonors(_cfg)
    CU.updateNewCourt(_cfg)
    
    
    #*********************************************
    print datetime.datetime.now(), datetime.datetime.now()-dt
    print 'End !'
    