#coding:utf8
'''
Created on Sep 19, 2016

@author: Administrator
'''
import datetime
import re
from HTMLParser import HTMLParser
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

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




