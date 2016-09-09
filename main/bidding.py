#encoding:utf8
'''
Created on Aug 31, 2016

@author: Administrator
'''
import pymongo
from pymongo import MongoClient
import time
import datetime
import re
from HTMLParser import HTMLParser

class MyHTMLParser(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d): self.fed.append(d)
    def html(self): return re.sub('\n| ', '', ''.join(self.fed)).strip()

def getHtml(html):
    parser = MyHTMLParser()
    parser.feed(html)
    return parser.html().encode('utf8')
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

def IsPrice(st):
    lsNum = dict((str(i),1) for i in range(0, 10))
    lsNum['.'] = 1
    st = re.sub(',|，|元', '', st)
    for p in st:
        if p not in lsNum: return 0
    return 1
#解析html中中标公司信息
def readHtmlInfo(html, line):
    tds = []; lrow = -1
    st = re.findall(r'(?=<tr)([\s\S]+?)(?<=<\/tr>)', html)
    for i in [9, 10, 11, 12]: 
        tds = re.findall(r'<td.*?>(.*?)<\/td>', st[i])
        if len(tds)>0  and ('第一名' == tds[0] or '第1名' == tds[0]):
            lrow = i; break 
    if lrow==-1 or (len(tds)!=5 and len(tds)!=8): return 0
    line['company_name'] = re.sub('/|&nbsp;', '', tds[1])
    price = re.sub('无|/|\\\|&nbsp;', '', tds[3].strip().encode('utf8')).strip()
    if price=='' or price=='.' or price=='元': price = re.sub('无|/|\\\|&nbsp;', '', tds[2].strip().encode('utf8')).strip() 
    if IsPrice(price) and price!='' and price!='.': price = str(float(re.sub(',|，|元', '', price)) / 10000) + '万'
    line['biddingPrice'] = price
    if len(tds)==5:
        while lrow<(len(st)-1):
            pss = re.findall(r'<td.*?>(.*?)<\/td>', st[lrow+1])
            if len(pss)>0 and re.sub('&nbsp;', '', pss[0].strip())=="项目负责人": 
                if len(pss)==6: line['architects'] = pss[1]
                break
            lrow += 1
    if len(tds)==8: line['architects'] = tds[5]
    return 1

def deal_gsi():
    con1 = MongoClient('localhost', 27017)
    con2 = MongoClient('192.168.3.45', 27017)
    con3 = MongoClient('171.221.173.154', 27017)
    db1 = con1['middle']
    db2 = con2['constructionDB']
    db3 = con3['jianzhu3']
    db4 = con1['jianzhu']
    write = db1.bidding
    
    fdn = ['projectName', 'biddingPrice', 'biddingDate', 'sourcesUrl', 'architects', 'content', 'company_name', 'type', 'sources', 'announcementId', 'updateTime']
    fdv = ['', '', '', '', '', '', '', '招标', '四川省政府政务服务和公共资源交易服务中心', '', datetime.datetime.now()]
    lsmd5 = {}
    lsAll = []
    index = 0
    
    #****************************************************************************************
    for item in db4.gs_invitationBid.find({}, {'detailHtml':0}):
        line = dict(map(lambda k, v : (k,v), fdn, fdv))
        line['projectName'] = item['announcementName']
        line['biddingDate'] = Date_F(item['publishTime']) 
        line['sourcesUrl'] = item['detailUrl']
        line['announcementId'] = item['announcementId']
#        line['content'] = getHtml(item['detailHtml'])
#        line['content'] = re.sub(r'MsoNormal|Generator|li|div|您现在的位置：首页交易服务平台工程建设招标公告|阅读次数：】【我要打印】【关闭|(?=.cs)([\s\S]*)(?<=})|\t|(\s)|(?=\/\*)([\s\S]*)(?<=\*\/)|(?={)([\s\S]*)(?<=})', '', line['content']) 
        lmd5 = ','.join([line['projectName'], line['biddingDate'], line['sourcesUrl'], line['announcementId']])
        if lmd5 in lsmd5: print lmd5
        lsmd5[lmd5] = 1
        lsAll.append(line)
        index += 1
    print "collect", len(lsAll), 'records'
    
    #****************************************************************************************
    lsmd5 = {}
    ErrIndex = 0
    fdv[7] = '中标'
    for item in db4.gs_bidCandidate.find(): #单位：元
        line = dict(map(lambda k, v : (k,v), fdn, fdv))
        line['projectName'] = item['announcementName']
        line['biddingDate'] = Date_F(item['publishTime']) 
        line['sourcesUrl'] = item['detailUrl']
        line['announcementId'] = item['announcementId']
#        line['content'] = getHtml(item['detailHtml'])
        lmd5 = ','.join([line['projectName'], line['biddingDate'], line['sourcesUrl'], line['announcementId']])
        if lmd5 in lsmd5: print lmd5
        lsmd5[lmd5] = 1
        index += 1
        st = re.findall('(?=<table id="_Sheet1")([\s\S]+?)(?<=<\/table>)', item['detailHtml'])
        if len(st)>0:
            ErrIndex += 1 ^ readHtmlInfo(st[0], line)
        else: 
            ErrIndex += 1
        lsAll.append(line)
    print ErrIndex, 'error records.'
    print "collect", len(lsAll), 'records'
    
    fdv[8] = '四川省住房和城乡建设厅'
    #****************************************************************************************
    lsmd5 = {}
    fdv[7] = '中标'
    for item in db2.gst_bidResult.find(): #单位：万元
        line = dict(map(lambda k, v : (k,v), fdn, fdv))
        line['projectName'] = item['projectName']
        line['biddingDate'] = Date_F(item['biddingMessage'][0]['biddingNoticeDate']) 
        line['announcementId'] = item['uuid']
        line['company_name'] = item['companyName']
        line['biddingPrice'] = item['biddingMessage'][0]['biddingPrice'] + '万'
        line['architects'] = item['biddingMessage'][0]['projectManager']
        line['content'] = item['projectBase'][0]['investmentScale']
        if 'detailUrl' in item: line['sourcesUrl'] = item['detailUrl']
        lmd5 = ','.join([line['projectName'], line['biddingDate'], line['announcementId'], line['company_name']])
        if lmd5 in lsmd5: print lmd5
        lsmd5[lmd5] = 1
        index += 1
        lsAll.append(line)
    print "collect", len(lsAll), 'records'
    
    #****************************************************************************************
    lsmd5 = {}
    fdv[7] = '中标'
    s=0
    for item in db2.gst_project.find(): #单位：万元
        line = dict(map(lambda k, v : (k,v), fdn, fdv))
        line['projectName'] = item['projectName']
        line['sourcesUrl'] = item['detailURL']
        line['announcementId'] = item['uuid']
        line['content'] = item['projectDetail'][0]['investmentScale'] + item['projectDetail'][0]['constructionLocation']
        if len(item['projectBidResults'])==0 and len(item['projectEnterprises'])==0:
            line['type'] = '招标'
            line['biddingDate'] = Date_F(item['publishDate']) 
            index += 1
            lsAll.append(line)
        if len(item['projectBidResults'])==0 and len(item['projectEnterprises'])!=0:
            b = item['projectConstructionPermits'][0]
            line['biddingDate'] = Date_F(b['contractSDate'])
            line['company_name'] = b['executionUnit']
            line['biddingPrice'] = b['contractPrice']
            index += 1
            lsAll.append(line)
        for b in item['projectBidResults']:
            line = dict(map(lambda k, v : (k,v), fdn, fdv))
            line['projectName'] = item['projectName']
            line['biddingDate'] = Date_F(b['biddingNoticeDate']) 
            line['announcementId'] = item['uuid']
            line['company_name'] = b['bidder']
            line['biddingPrice'] = b['biddingPrice'] + '万'
            line['architects'] = b['projectManager']
            line['sourcesUrl'] = item['detailURL']
            line['content'] = item['projectDetail'][0]['investmentScale'] + item['projectDetail'][0]['constructionLocation']
            if line['company_name']+b['biddingPrice']=='': continue
            lmd5 = ','.join([line['projectName'], line['biddingDate'], line['announcementId'], line['company_name']])
            if lmd5 in lsmd5: pass;#print lmd5
            lsmd5[lmd5] = 1
            index += 1
            lsAll.append(line)
    print "collect", len(lsAll), 'records'
    
    #*************************************写数据*************************************
    index = 60000001
    lsCompany = {}
    for item in db3.companyInfoNew.find({}).limit(1):lskey = dict((key, 0) for key in item if key!='company_name' and key !='id') 
    for item in db3.companyInfoNew.find({}, lskey): lsCompany[item['company_name'].encode('utf8')] = item['id']
    for line in lsAll:
        cpname = line['company_name'].encode('utf8')
        line['id'] = index
        line['label'] = 0
        line['company_id'] = lsCompany[cpname] if cpname in lsCompany else 0 
        index += 1
#    write.insert(lsAll)
    
def getContent(line):
    pass
#    for item in db2.gs_bidCandidate.find():
#        pass
#    for item in db2.gst_bidResult.find():pass
#    for item in db2.gst_project.find():pass
    
#    print index

def Date_F(str):
#    str = str.decode('utf8')
    if not str or str == '': return ''
    if not (str.find("年")>0 and str.find("月")>0): return str
    sp = re.split("年|月".decode('utf8'), str)
    return (sp[0] + '-' + sp[1] + '-' + sp[2].replace('日', '')).encode('utf8')


if __name__ == '__main__':
    dt = datetime.datetime.now()
    
    deal_gsi()

    print datetime.datetime.now(), datetime.datetime.now()-dt