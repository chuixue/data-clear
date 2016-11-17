#coding:utf8
'''
Created on Sep 1, 2016

@author: Administrator
'''
import re
#import urllib2,re
#import time,sys
from HTMLParser import HTMLParser
#设置默认编码
#type = sys.getfilesystemencoding()
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


class MyHTMLParser(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)
        return re.sub('资质|--请选择--| ', '', ''.join(self.fed))
 
     
def main():
    parser = MyHTMLParser()
    
    
    parser.feed('<style type="text/css">#python { color: green }</style>')
    print parser.get_data().strip()
             
if __name__ == '__main__':
    main()

    
    pass