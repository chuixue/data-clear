#coding:utf8
'''
Created on Dec 12, 2016

@author: Administrator
'''
from pymongo import MongoClient
import sys
import config
import time, datetime
import os
import json

reload(sys)
sys.setdefaultencoding('utf-8')


class Log(object):
    '''
        'updateTime':'', 'message':'', 'content':'', 'status':0, 'source':'', 'target':'' 
        'error':0, 'label':'', 'style':'', 'mark':''
        
    '''
    def __init__(self):
        self.data = []
        self.cfg = config.Config() 
        if not os.path.exists('logs'): os.mkdir('logs')
        self.writer = open(time.strftime('logs//log%Y%m%d%H.txt'), 'a')
        
    def add(self, content):
        if type(content)==type(""):
            content = {'content': content}
        elif type(content)==type(2):
            content = {'content': str(content)}
        elif type(content)==type({}):
            content = content
        content['updateTime'] = datetime.datetime.now()
        self.data.append(content)
        if len(self.data)>200: self.save()
            
    def save(self):
        if len(self.data)>0:
            self.cfg.tbLog.insert(self.data)
        self.data = []
    
    def error(self):
        for line in self.data:
            print json.dumps(line) + '\r\n'
            self.writer.write(json.dumps(line) + '\r\n')
        self.writer.close()
        self.data = []
    
    def Log(self):
        pass
    
    def __del__(self):
        if len(self.data)>0: self.error()
        
        
        
        
if __name__ == '__main__':
    '''
    lg = Log()
    lg.add('test')
    lg.save()
    '''
    
    pass