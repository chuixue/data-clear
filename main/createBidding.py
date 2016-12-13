#coding:utf8
'''
Created on Sep 14, 2016

@author: Administrator
'''
from pymongo import MongoClient
import datetime
import re
import public as P
import libBidding as libB
import copy
import config as CFG
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def log(str):
    fp = open('c:\\t.txt', 'a')
    fp.write(str)
    fp.write("\r\n")
    fp.close()
def cout(ls):
    for l in ls: print l, ':', ls[l],
    print
def out(ls):
    for l in ls: print l,
    print


def getLine(_title, _source): 
    return {'name':_title, 'source':_source, 'url':'', 'companys':[], 'price':'', 'architects':'', 'content':'', 'type':'' }

#-------------------------------------四张表业绩信息-------------------------------------   
class ReadBidding(object):
    def __init__(self, cfg):
        self.lsBidding = []
        self.cfg = cfg
        self.db = self.cfg.dbBidding
        self.fdn = ['projectName', 'biddingPrice', 'biddingDate', 'sourcesUrl', 'architects', 'content', 'company_name', 'type', 'sources', 'announcementId', 'updateTime']
        self.fdv = ['', '', '', '', '', '', '', '招标', '四川省政府政务服务和公共资源交易服务中心', '', datetime.datetime.now()]
    def log(self): print '--', len(self.lsBidding), 'Records collect.'
    
    def read_gst_bidResult(self):
        return self._read_gst_bidResult(self.cfg.dbBidding.gst_bidResult.find())
    def read_gs_bidCandidate(self):
        return self._read_gs_bidCandidate(self.cfg.dbBidding.gs_bidCandidate.find())
    def read_gs_invitationBid(self):
        return self._read_gs_invitationBid(self.cfg.dbBidding.gs_invitationBid.find({}, {'detailHtml':0}))
    def read_gst_project(self):
        return self._read_gst_project(self.cfg.dbBidding.gst_project.find())
    
    
    def _read_gs_invitationBid(self, cursor):
        lsmd5 = {}
        for item in cursor:
            line = dict(map(lambda k, v : (k,v), self.fdn, self.fdv))
            line['projectName'] = item['announcementName']
            line['biddingDate'] = P.Date_F(item['publishTime']) 
            line['sourcesUrl'] = item['detailUrl']
            line['announcementId'] = item['announcementId']
            #line['content'] = getHtml(item['detailHtml'])
            #line['content'] = re.sub(r'MsoNormal|Generator|li|div|您现在的位置：首页交易服务平台工程建设招标公告|阅读次数：】【我要打印】【关闭|(?=.cs)([\s\S]*)(?<=})|\t|(\s)|(?=\/\*)([\s\S]*)(?<=\*\/)|(?={)([\s\S]*)(?<=})', '', line['content']) 
            lmd5 = ','.join([line['projectName'], line['sourcesUrl']])
            if lmd5 in lsmd5: print lmd5
            lsmd5[lmd5] = 1
            self.lsBidding.append(line)
            if len(self.lsBidding)>1000: break  #----------------------------------------------------
            
        self.log()
    
    def _read_gs_bidCandidate(self, cursor):
        lsmd5 = {}
        ErrIndex = 0
        fdv = copy.deepcopy(self.fdv)
        fdv[7] = '中标'
        for item in cursor: #单位：元
            line = dict(map(lambda k, v : (k,v), self.fdn, fdv))
            line['projectName'] = re.sub('中标公示', '', item['announcementName'].encode('utf8'))
            line['biddingDate'] = P.Date_F(item['publishTime']) 
            line['sourcesUrl'] = item['detailUrl']
            line['announcementId'] = item['announcementId']
            #line['content'] = getHtml(item['detailHtml'])
            lmd5 = ','.join([line['projectName'], line['sourcesUrl']])
            if lmd5 in lsmd5: print lmd5
            lsmd5[lmd5] = 1
            st = re.findall('(?=<table id="_Sheet1")([\s\S]+?)(?<=<\/table>)', item['detailHtml'])
            ErrIndex += 1 ^ libB.readHtmlInfo(st[0], line) if len(st)>0 else 1
            if not len(st)>0 or 'company_name' not in line or len(line['company_name'])<2: continue
            self.lsBidding.append(line)
            if len(self.lsBidding)>2000: break  #----------------------------------------------------
            
        print '--', ErrIndex, 'error records.'
        self.log()
    
    def _read_gst_bidResult(self, cursor):
        lsmd5 = {}
        fdv = copy.deepcopy(self.fdv)
        fdv[7] = '中标'
        fdv[8] = '四川省住房和城乡建设厅'
        index = 0
        for item in cursor: #单位：万元
            line = dict(map(lambda k, v : (k,v), self.fdn, fdv))
            line['projectName'] = item['projectName'].encode('utf8')
            line['biddingDate'] = P.Date_F(item['biddingMessage'][0]['biddingNoticeDate']) 
            line['announcementId'] = item['uuid']
            line['company_name'] = item['companyName']
            line['biddingPrice'] = item['biddingMessage'][0]['biddingPrice'] + '万'
            line['architects'] = item['biddingMessage'][0]['projectManager']
            line['content'] = item['projectBase'][0]['investmentScale']
            if 'detailUrl' in item: line['sourcesUrl'] = item['detailUrl'].strip()
            lmd5 = ','.join([line['projectName'], line['biddingDate'], line['announcementId'], line['company_name']])
            if lmd5 in lsmd5: print lmd5
            lsmd5[lmd5] = 1
            index += 1
            self.lsBidding.append(line)
        self.log()
            
    def _read_gst_project(self, cursor):
        lsmd5 = {}
        fdv = copy.deepcopy(self.fdv)
        fdv[8] = '四川省住房和城乡建设厅'
        fdv[7] = '中标'
        for item in cursor: #单位：万元
            line = dict(map(lambda k, v : (k,v), self.fdn, fdv))
            line['projectName'] = item['projectName'].encode('utf8')
            line['sourcesUrl'] = item['detailURL']
            line['announcementId'] = item['uuid']
            line['content'] = item['projectDetail'][0]['investmentScale'] + item['projectDetail'][0]['constructionLocation']
            if len(item['projectBidResults'])==0 and len(item['projectEnterprises'])==0:
                line['type'] = '招标'
                line['biddingDate'] = P.Date_F(item['publishDate']) 
                self.lsBidding.append(line)
            if len(item['projectBidResults'])==0 and len(item['projectEnterprises'])!=0:
                b = item['projectConstructionPermits'][0]
                line['biddingDate'] = P.Date_F(b['contractSDate'])
                line['company_name'] = b['executionUnit']
                line['biddingPrice'] = b['contractPrice']
                self.lsBidding.append(line)
            for b in item['projectBidResults']:
                line = dict(map(lambda k, v : (k,v), self.fdn, fdv))
                line['projectName'] = item['projectName']
                line['biddingDate'] = P.Date_F(b['biddingNoticeDate']) 
                line['announcementId'] = item['uuid']
                line['company_name'] = b['bidder']
                line['biddingPrice'] = b['biddingPrice'] + '万'
                line['architects'] = b['projectManager']
                line['sourcesUrl'] = item['detailURL']
                line['content'] = item['projectDetail'][0]['investmentScale'] + item['projectDetail'][0]['constructionLocation']
                if line['company_name']+b['biddingPrice']=='': continue
                lmd5 = ','.join([line['projectName'], line['biddingDate'], line['company_name']])
                if lmd5 in lsmd5: continue
                lsmd5[lmd5] = 1
                self.lsBidding.append(line)
        self.log()
        
        
def remove_repeat(lines):
    lsData = []
    lsjstId = {}
    lsCPS = {}
    lsTemp4 = {}
    lsTemp3 = {}
    for line in lines:
        if line['type']=='招标': 
            lsData.append(line)
            continue
        '''id去重'''
        ida = re.findall('(?<=id\=)([\s\S]+?)(?<=$)', line['sourcesUrl']); 
        id = ida[0] if len(ida)>0 else ''   
        pid = id + line['company_name']        
        if id!='' and pid in lsjstId: continue
        if id!='': lsjstId[pid] = line
        '''项目名、公司名、链接判断去重'''    
        cpname = line['company_name'] 
        if line['projectName'][-6:]=='施工': line['projectName'] = line['projectName'][: -6]
        cps = '_'.join([line['company_name'], line['projectName'], line['sourcesUrl']])
        if cps in lsCPS: continue
        lsCPS[cps] = line
        '''公司名、价格、负责人、日期'''
        if checkRepeat(line, lsTemp4, lsTemp3): continue
        if '中标'==''.join([line['type'], line['projectName'], line['architects'], line['biddingPrice']]).strip(): continue
        lsData.append(line)
    print len(lsData) 
    return lsData

def checkRepeat(line, lsTemp4, lsTemp3):
    R = False
    cpname = line['company_name']
    cdate = P.dateFormat(line['biddingDate'])
    cdpps = [cpname, line['architects'], line['biddingPrice']]
    if len([x for x in cdpps if x!=''])==3 and cdate!='':
        cpp = '_'.join(cdpps).encode('utf8')
        cdpp = cdate + '_' + cpp
        if cpp in lsTemp3:
            if cdpp in lsTemp4: 
                R = True
            else:
                ldt = [P.dateToStr(P.strToDate(cdate) + datetime.timedelta(days=i)) for i in range(1, 5)]
                ldt += [P.dateToStr(P.strToDate(cdate) - datetime.timedelta(days=i)) for i in range(1, 5)]
                _flag = False
                for d in ldt:
                    if d + '_' + cpp in lsTemp4: _flag = True; break
                if _flag: R = True                            
        if not R: 
            lsTemp3[cpp] = 1
            lsTemp4[cdpp] = 1
    return R

def readBidding(cfg): 
    rb = ReadBidding(cfg)
    
    print 'read and deal table：gs_invitationBid'
    rb.read_gs_invitationBid()
    
    print 'read and deal table：gs_bidCandidate'
    rb.read_gs_bidCandidate()
    
    print 'read and deal table：gst_bidResult'
    rb.read_gst_bidResult()
    
    print 'read and deal table：gst_project'
    rb.read_gst_project()
    
    remove_repeat(rb.lsBidding)
        
    return rb.lsBidding

'''写入数据库'''
def writeBidding(cfg, lsBidding):
    index = 60000001
    lsCpId = P.getCompanyId(cfg.companyInfo)
    for line in lsBidding:
        cpname = line['company_name'].encode('utf8')
        line['id'] = index
        line['label'] = 0
        line['company_id'] = 0 if cpname not in lsCpId else lsCpId[cpname]
        index += 1
    cfg.writeBidding.insert(lsBidding)

  
#将companyAchievement表业绩统一整理到业绩表   
def addBiddingFromcompanyAchievement(cfg):    
    lsComp = {}
    lsTemp = {}
    lsTemp3 = {}
    lsTemp4 = {}
    lsUpdate = []
    source = cfg.companyAchievement
    index = P.getMaxId(cfg.writeBidding, 'id') + 1
    print 'select company list for id.'
    lsCpId = P.getCompanyId(cfg.companyInfo)
    print 'select useful company.'
    for item in source.find({}, P.dbKeys(source, ['companyName'])): 
        lsComp[item['companyName'].encode('utf8')] = 1
    print 'read existing bidding of company.'
    for item in cfg.tbBidding.find({'type':'中标'}):
        cpname = item['company_name'].encode('utf8')
        pj = item['projectName'].encode('utf8')
        url = item['sourcesUrl'].encode('utf8')
        if cpname=='' or cpname not in lsComp: continue
        if cpname!='' and url!='': lsTemp[cpname+'_'+'_'+url] = 1
        '''--------------二次去重准备------------'''
        cdate = P.dateFormat(item['biddingDate'])
        cdpps = [cpname, item['architects'], item['biddingPrice']]
        cpp = '_'.join(cdpps).encode('utf8')
        cdpp = cdate + '_' + cpp
        if len([x for x in cdpps if x!=''])!=3 or cdate=='': continue
        lsTemp3[cpp] = 1
        lsTemp4[cdpp] = 1  
        '''-----------------------------------'''
    print 'read new bidding of company.'
    cpl = ['sources', 'sourcesUrl', 'biddingDate', 'biddingPrice', 'projectName', 'architects']
    for item in source.find({'biddingDetail':{'$gt':[]}}):
        cpname = item['companyName'].encode('utf8')
        for b in item['biddingDetail']:
            pj = b['projectName'][:-4].strip().encode('utf8')
            if pj[-6:]=='施工': pj = pj[: -6]
            '''--------------全局去重------------'''
            url = b['sourcesUrl'].strip().encode('utf8')
            if url!='': 
                if cpname+'_'+'_'+url in lsTemp: continue
                lsTemp[cpname+'_'+'_'+url] = 1
            '''-----------------------------------'''
            line = dict((k, b[k]) for k in cpl)            
            line['biddingPrice'] = re.sub('暂无信息', '', line['biddingPrice'].strip().encode('utf8'))
            line['architects'] = re.sub('暂无信息', '', line['architects'].strip().encode('utf8'))
            line['projectName'] = pj
            line['company_name'] = cpname
            line['updateTime'] = datetime.datetime.now()
            line['type'] = '中标'
            line['content'] = ''
            line['announcementId'] = ''
            line['label'] = 0
            line['company_id'] = 0 if cpname not in lsCpId else lsCpId[cpname]
            '''--------------二次去重------------'''
            if checkRepeat(line, lsTemp4, lsTemp3): continue
            if '中标'==''.join([line['type'], line['projectName'], line['architects'], line['biddingPrice']]).strip():
                print 'delete'
                continue
            
            '''-----------------------------------'''
            line['id'] = index
            index += 1 
            lsUpdate.append(line)
    print 'insert new records,',len(lsUpdate), '...'
    cfg.writeBidding.insert(lsUpdate)
    print 'complete!'

def addCompanyBiddingCount(cfg):
    print 'update company bidding count...'
    reducer = """
                   function(obj, prev){
                       prev.count++;
                   }
            """
    results = cfg.tbBidding.group(key={"company_name":1}, condition={'type':'中标', 'company_name':{'$gt':''}}, initial={"count": 0}, reduce=reducer)
    index = 0
    lsComp = {}
    lsUpdate = []
    for item in results: lsComp[item['company_name'].encode('utf8')] = int(item['count']) 
    for item in cfg.companyInfo.find({}, P.dbKeys(cfg.companyInfo, ['id', 'company_name'])):
        cpname = item['company_name'].encode('utf8')
        count = 0 if cpname not in lsComp else lsComp[cpname]
        lsUpdate.append([{'id':item['id']}, {'$set':{"biddingCount": count, 'updateTime':datetime.datetime.now()}}])
    for d in lsUpdate:
        index += 1
        if index % 5000 ==0: print d[0], index
        cfg.companyInfo.update(d[0], d[1])
    print '共更新公司总数：',index
    

if __name__ == '__main__':
    dt = datetime.datetime.now()
    
    _cfg = CFG.Config()
    
    writeBidding(_cfg, readBidding(_cfg))
    addBiddingFromcompanyAchievement(_cfg)
    addCompanyBiddingCount(_cfg)
    
#    deal_gsi()
#    addCompanyBidding()
    
#    write.ensure_index('id')
#    write.ensure_index('company_id')
    
    print datetime.datetime.now(), datetime.datetime.now()-dt
