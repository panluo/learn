#!/usr/bin/env python
from urlparse import urlparse   
import re
import sys

class get_url:

    domain_set = {}
    black_list = []
    def __init__(self,old="/home/luo/sample/url",new="/home/luo/sample/data",black_list="/home/luo/sample/black_list"):
        self.old = old
        self.new = new
        self.black_list = black_list
        
        self.load_old()
        self.load_black_list()

    def update_url():
        

    def load_old(self):
        try:
            url_old = open(self.old,"r")
        except Exception as e:
            print "file not exsits"
            print e
            sys.exit()
        tmp = []
        for url in url_old:
            domain = get_top_host(url)
            if domain_set.has_key(domain):
                domain_set[domain].append(url)
            else:
                domain_set[domain] = tmp.append(url)
        
        url_old.close()

    def load_black_list(self):
        try:
            black_list = open(self.black_list,"r")
        except Exception as e:
            print "file not exsits"
            print e
            sys.exit()
            
        for item in black_list:
            black_list.append(item)

        black_list.close()

    
    def get_top_host(self,url):
        
        extractPattern = r'(\w+)://([w]{3}.)?(.+)/'
        pattern = re.compile(extractPattern,re.IGNORECASE)
        m = pattern.match(url)
        return m.group(3) if m else host
    
    
    def __del__(self):
        pass

if __name__ == "__main__":
    a = get_url()
