#coding:utf8
'''
Created on Sep 29, 2016

@author: Administrator
'''
import datetime
import createBidding as CB
import libLog as LG
import config as CFG
import public as P

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def getBiddingOriginal(cfg):
    lsB1 = {}       #招标
    lsB2 = {}       #中标
    for item in cfg.updateBidding.find():
    #for item in cfg.dbNow['biddingC'].find():        
        tp = item['type'].encode('utf8')
        if tp=='招标':
            lmd5 = ','.join([item['projectName'], item['sourcesUrl']]).encode('utf8')
            lsB1[lmd5] = 1
        else:
            lmd5 = ''.join([item[k].encode('utf8') for k in ['company_name', 'projectName', 'biddingPrice', 'biddingDate', 'architects']])
            lsB2[lmd5] = item
            
    print 'original records count', len(lsB1), len(lsB2)  
    return lsB1, lsB2


class readBiddingData(object):
    def __init__(self, cfg):
        self.cfg = cfg
        self.db = self.cfg.dbBidding
        self.RB = CB.ReadBidding(cfg)
        self.callback = {'gs_invitationBid':self.RB._read_gs_invitationBid, 'gs_bidCandidate':self.RB._read_gs_bidCandidate,
                         'gst_bidResult':self.RB._read_gst_bidResult, 'gst_project':self.RB._read_gst_project,
                         }
    '''可根据需要设置数据过滤条件'''
    def readBiddingDataFilter(self, tb):
        cursor = self.db[tb].find({})#.limit(2000)
        return self.callback[tb](cursor)

def checkNewRecord(cfg, newBiddingList):
    lg = LG.Log()
    lg.log('select the max id.')
    lsCompany = P.getCompanyId(cfg.companyInfo)
    index = P.getMaxId(cfg.updateBidding, 'id') + 1
    if index==0: index = 60000001
    lsB1, lsB2 = getBiddingOriginal(cfg)
    lsNew = []
    for item in newBiddingList:
        if item['type']=='招标':
            lmd5 = ','.join([item['projectName'], item['sourcesUrl']]).encode('utf8') 
            if lmd5 in lsB1: continue
            lsNew.append(item)
        else:
            lmd5 = ''.join([item[k].encode('utf8') for k in ['company_name', 'projectName', 'biddingPrice', 'biddingDate', 'architects']])
            if lmd5 in lsB2: continue
            lsNew.append(item)
        '''End if'''
    '''End for'''
    lg.log(len(newBiddingList), 'remove repeat records left count', len(lsNew))
    '''三次去重'''
    lsNew.extend(lsB2.values())             #新+旧
    biddingList = CB.remove_repeat(lsNew)   #去重
    lsData = []
    for item in biddingList:
        if item['type']=='中标':
            lmd5 = ''.join([item[k].encode('utf8') for k in ['company_name', 'projectName', 'biddingPrice', 'biddingDate', 'architects']])
            if lmd5 in lsB2: continue
        cpname = item['company_name'].encode('utf8')
        item['id'] = index
        item['label'] = 0
        item['other'] = ""
        item['company_id'] = 0 if cpname not in lsCompany else lsCompany[cpname]
        index += 1
        lsData.append(item)
    lg.log(len(newBiddingList), 'remove repeat records left count', len(lsData))
    lg.save()
    return lsData

def readBidding(cfg): 
    lg = LG.Log()
    rb = readBiddingData(cfg)
    tables = ['gs_invitationBid', 'gs_bidCandidate', 'gst_bidResult', 'gst_project']
    for tb in tables:
        '''依次处理各表'''
        lg.log( 'read and deal table：', tb)
        rb.readBiddingDataFilter(tb)
        
    '''数据去重'''
    biddingList = CB.remove_repeat(rb.RB.lsBidding)
    ''''''
    data = checkNewRecord(cfg, biddingList)
    lg.save()
    return data


def updateBidding(cfg, dataset):
    lg = LG.Log() 
    lg.log('insert into table', len(dataset), ' ...')
    if len(dataset)>0: cfg.updateBidding.insert(dataset)
    lg.log('completed!')
    lg.save()
    
    
if __name__ == '__main__':
    dt = datetime.datetime.now()
    #*********************************************
    _cfg = CFG.Config()
    

    updateBidding(_cfg, readBidding(_cfg))
    CB.addBiddingFromcompanyAchievement(_cfg)
    CB.addCompanyBiddingCount(_cfg)
    
        

    
    #*********************************************
    print datetime.datetime.now(), datetime.datetime.now()-dt
    print 'End !'
    
    

