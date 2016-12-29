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
import types

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
        
    def add(self, msg, p1 = "", p2 = "", p3 = "", p4 = ""):
        content = {}
        _content = msg if type(msg)==type({}) or type(msg)==type([]) else str(msg).encode('utf8')  
        if type(_content)==type("") or type(_content)==type("中文") or type(_content)==type(1):
            _content = str(_content) + ''.join([p if p=="" else ", "+ p for p in [str(p1), str(p2), str(p3), str(p4)]])
            content = {'content': str(_content)}
            if p1!=False: print _content
        elif type(_content)==type({}):
            content = _content
        else:
            return
        content['updateTime'] = time.strftime('%Y-%m-%d %H:%M:%S')
        self.data.append(content)
        if len(self.data)>200: self.save()
        
    def log(self, content, p1 = "", p2 = "", p3 = "", p4 = ""):
        self.add(content, p1, p2, p3, p4)
              
    def save(self):
        if len(self.data)>0:
            self.cfg.tbLog.insert(self.data)
            self.data[:] = []
            
    def error(self):
        for line in self.data: 
            self.writer.write(json.dumps(line) + '\r\n')
        self.writer.close()
        self.data[:] = []
    
    def Log(self):
        pass
    
    def __del__(self):
        if len(self.data)>0: 
            self.error()
        self.writer.close()
        
        
        
if __name__ == '__main__':
    '''
    lg = Log()
    lg.add('test')
    lg.save()
    '''
    
    pass