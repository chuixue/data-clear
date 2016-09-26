#coding:utf8
'''
Created on Sep 14, 2016

@author: Administrator
'''
from pymongo import MongoClient
import datetime
import re
from HTMLParser import HTMLParser
import public as P
import libBidding as libB

#class MyHTMLParser(HTMLParser):
#    def __init__(self):
#        self.reset()
#        self.fed = []
#    def handle_data(self, d): self.fed.append(d)
#    def html(self): return re.sub('\n| ', '', ''.join(self.fed)).strip()
#
#def getHtml(html):
#    parser = MyHTMLParser()
#    parser.feed(html)
#    return parser.html().encode('utf8')

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

#def IsPrice(st):
#    lsNum = dict((str(i),1) for i in range(0, 10))
#    lsNum['.'] = 1
#    st = re.sub(',|，|元', '', st)
#    for p in st:
#        if p not in lsNum: return 0
#    return 1
##解析html中中标公司信息
#def readHtmlInfo(html, line):
#    tds = []; lrow = -1
#    st = re.findall(r'(?=<tr)([\s\S]+?)(?<=<\/tr>)', html)
#    for i in [9, 10, 11, 12]: 
#        tds = re.findall(r'<td.*?>(.*?)<\/td>', st[i])
#        if len(tds)>0  and ('第一名' == tds[0] or '第1名' == tds[0]):
#            lrow = i; break 
#    if lrow==-1 or (len(tds)!=5 and len(tds)!=8): return 0
#    line['company_name'] = re.sub('/|&nbsp;', '', tds[1])
#    price = re.sub('无|/|\\\|&nbsp;', '', tds[3].strip().encode('utf8')).strip()
#    if price=='' or price=='.' or price=='元': price = re.sub('无|/|\\\|&nbsp;', '', tds[2].strip().encode('utf8')).strip() 
#    if IsPrice(price) and price!='' and price!='.': price = str(float(re.sub(',|，|元', '', price)) / 10000) + '万'
#    line['biddingPrice'] = price
#    if len(tds)==5:
#        while lrow<(len(st)-1):
#            pss = re.findall(r'<td.*?>(.*?)<\/td>', st[lrow+1])
#            if len(pss)>0 and re.sub('&nbsp;', '', pss[0].strip())=="项目负责人": 
#                if len(pss)==6: line['architects'] = pss[1]
#                break
#            lrow += 1
#    if len(tds)==8: line['architects'] = tds[5]
#    return 1
#四张表业绩信息    
def deal_gsi():
    fdn = ['projectName', 'biddingPrice', 'biddingDate', 'sourcesUrl', 'architects', 'content', 'company_name', 'type', 'sources', 'announcementId', 'updateTime']
    fdv = ['', '', '', '', '', '', '', '招标', '四川省政府政务服务和公共资源交易服务中心', '', datetime.datetime.now()]
    lsmd5 = {}  #'''检测单表重复'''
    lsAll = []  
    lsCPS = {}  #'''检测全局重复'''
    index = 0
    
    
    #****************************************************************************************
    for item in db5.gs_invitationBid.find({}, {'detailHtml':0}):
        line = dict(map(lambda k, v : (k,v), fdn, fdv))
        line['projectName'] = item['announcementName']
        line['biddingDate'] = P.Date_F(item['publishTime']) 
        line['sourcesUrl'] = item['detailUrl']
        line['announcementId'] = item['announcementId']
        #line['content'] = getHtml(item['detailHtml'])
        #line['content'] = re.sub(r'MsoNormal|Generator|li|div|您现在的位置：首页交易服务平台工程建设招标公告|阅读次数：】【我要打印】【关闭|(?=.cs)([\s\S]*)(?<=})|\t|(\s)|(?=\/\*)([\s\S]*)(?<=\*\/)|(?={)([\s\S]*)(?<=})', '', line['content']) 
        lmd5 = ','.join([line['projectName'], line['sourcesUrl']])
        if lmd5 in lsmd5: print lmd5
        lsmd5[lmd5] = 1
        lsAll.append(line)
        index += 1
    print "collect", len(lsAll), 'records'

    #****************************************************************************************
    lsmd5 = {}
    ErrIndex = 0
    fdv[7] = '中标'
    for item in db5.gs_bidCandidate.find(): #单位：元
        line = dict(map(lambda k, v : (k,v), fdn, fdv))
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
#        '''--------------全局去重------------'''
#        if line['projectName'][-6:]=='施工': line['projectName'] = line['projectName'][: -6]
#        cps = '_'.join([line['company_name'], line['projectName'], line['sourcesUrl']])
#        if cps in lsCPS: continue
#        lsCPS[cps] = 1
#        '''-----------------------------------'''
        lsAll.append(line)
    print ErrIndex, 'error records.'
    print "collect", len(lsAll), 'records'
    
    lsjstId = {}
    fdv[8] = '四川省住房和城乡建设厅'
    #****************************************************************************************
    lsmd5 = {}
    fdv[7] = '中标'
    for item in db2.gst_bidResult.find(): #单位：万元
        line = dict(map(lambda k, v : (k,v), fdn, fdv))
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
        ida = re.findall('(?<=id\=)([\s\S]+?)(?<=$)', line['sourcesUrl']); id = ida[0] if len(ida)>0 else ''   
        if id!='': lsjstId[id+line['company_name']] = 1
#        '''--------------全局去重------------'''
#        if line['projectName'][-6:]=='施工': line['projectName'] = line['projectName'][: -6]
#        cps = '_'.join([line['company_name'], line['projectName'], line['sourcesUrl']])
#        if cps in lsCPS: continue
#        lsCPS[cps] = 1
#        '''-----------------------------------'''
        index += 1
        lsAll.append(line)
    print "collect", len(lsAll), 'records'
    
    #****************************************************************************************
    lsmd5 = {}
    fdv[7] = '中标'
    for item in db2.gst_project.find(): #单位：万元
        line = dict(map(lambda k, v : (k,v), fdn, fdv))
        line['projectName'] = item['projectName'].encode('utf8')
        line['sourcesUrl'] = item['detailURL']
        line['announcementId'] = item['uuid']
        line['content'] = item['projectDetail'][0]['investmentScale'] + item['projectDetail'][0]['constructionLocation']
        if len(item['projectBidResults'])==0 and len(item['projectEnterprises'])==0:
            line['type'] = '招标'
            line['biddingDate'] = P.Date_F(item['publishDate']) 
            index += 1
            lsAll.append(line)
        if len(item['projectBidResults'])==0 and len(item['projectEnterprises'])!=0:
            b = item['projectConstructionPermits'][0]
            line['biddingDate'] = P.Date_F(b['contractSDate'])
            line['company_name'] = b['executionUnit']
            line['biddingPrice'] = b['contractPrice']
            index += 1
            lsAll.append(line)
        for b in item['projectBidResults']:
            line = dict(map(lambda k, v : (k,v), fdn, fdv))
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
            '''''''''去掉id重复的'''''''''''''''''''''
            ida = re.findall('(?<=id\=)([\s\S]+?)(?<=$)', line['sourcesUrl']); id = ida[0] if len(ida)>0 else ''   
            pid = id+line['company_name']
            if id!='' and pid in lsjstId: continue
            if id!='': lsjstId[pid] = 1
            ''''''''''''''''''''''''''''''''''''''''''
#            '''--------------全局去重------------'''
#            if line['projectName'][-6:]=='施工': line['projectName'] = line['projectName'][: -6]
#            cps = '_'.join([line['company_name'], line['projectName'], line['sourcesUrl']])
#            if cps in lsCPS: continue
#            lsCPS[cps] = 1
#            '''-----------------------------------'''
            index += 1
            lsAll.append(line)
    print "collect", len(lsAll), 'records'
    lsProj = []
    lsTemp4 = {}
    lsTemp3 = {}
    '''--------------全局去重------------'''
    for line in lsAll:
        if line['type']=='招标': 
            lsProj.append(line)
            continue
        cpname = line['company_name'] 
        if line['projectName'][-6:]=='施工': line['projectName'] = line['projectName'][: -6]
        cps = '_'.join([line['company_name'], line['projectName'], line['sourcesUrl']])
        if cps in lsCPS: continue
        lsCPS[cps] = 1 
        if checkRepeat(line, lsTemp4, lsTemp3): continue
        lsProj.append(line)
    '''-----------------------------------'''
    
#    return
    #*************************************写数据*************************************
    lsAll = lsProj
    index = 60000001
    lsCpId = P.getCompanyId(companyInfo)
    for line in lsAll:
        cpname = line['company_name'].encode('utf8')
        line['id'] = index
        line['label'] = 0
        line['company_id'] = 0 if cpname not in lsCpId else lsCpId[cpname]
        index += 1
    write.insert(lsAll)

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

#将companyAchievement表业绩统一整理到业绩表   
def addCompanyBidding():    
    source = db2.companyAchievement
    lsComp = {}
    lsTemp = {}
    lsTemp3 = {}
    lsTemp4 = {}
    lsUpdate = []
    index = P.getMaxId(write, 'id') + 1
    print 'select company list for id.'
    lsCpId = P.getCompanyId(companyInfo)
    print 'select useful company.'
    for item in source.find({}, P.dbKeys(source, ['companyName'])): 
        lsComp[item['companyName'].encode('utf8')] = 1
    print 'read existing bidding of company.'
    for item in bidding.find({'type':'中标'}):
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
            '''-----------------------------------'''
            line['id'] = index
            index += 1 
            lsUpdate.append(line)
    print 'insert new records,',len(lsUpdate), '...'
    write.insert(lsUpdate)
    print 'complete!'

if __name__ == '__main__':
    dt = datetime.datetime.now()
    con1 = MongoClient('localhost', 27017)
    con2 = MongoClient('192.168.3.45', 27017)
    #con3 = MongoClient('171.221.173.154', 27017)
    con4 = MongoClient('192.168.3.221', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    #db3 = con3['jianzhu3']
    db5 = con1['jianzhu']
    db4 = con4['jianzhu3']
    
    bidding = db1.bidding
    write = db1.bidding
    companyInfo = db1.companyInfoNew
    
#    deal_gsi()
#    addCompanyBidding()
    
    write.ensure_index('id')
    write.ensure_index('company_id')
    
    print datetime.datetime.now(), datetime.datetime.now()-dt




if __name__ == '__main__':
    pass