#!/usr/bin/env python

import sys,urllib2
import time
from urllib2 import URLError
from  xml.dom import minidom

def get_attrivalue(node,attrname):
    return node.getAttribute(attrname) if node.hasAttribute(attrname) else ''

def get_nodevalue(node, index=0):
    return node.childNodes[index].nodeValue if node else ''

def get_xmlnode(node, name):
    return node.getElementsByTagName(name) if node else []


def get_xml_data(url,output):
    total_url = "http://data.alexa.com/data?dat=snbamz&cli=10&url="+url

    result_dict = {}
    time.sleep(5)
    
    try:
        wp = urllib2.urlopen(total_url,timeout=200)
        content = wp.read()
        doc = minidom.parseString(content)
        root = doc.documentElement
    except Exception,e:
        output.write(url)
    else:
        if wp.getcode() != 200:
            print "error when open url"
            return result_dict

        SDs = get_xmlnode(root,"SD")
        if len(SDs) >=2:
            SD = SDs[1]
        else:
            return result_dict
        
        populars = get_xmlnode(SD,"POPULARITY")
        if len(populars) > 0:
            popular = populars[0]
            
            result_dict["popularity_url"] = get_attrivalue(popular,"URL")
            result_dict["popularity_text"] = get_attrivalue(popular,"TEXT")
            result_dict["popularity_source"] = get_attrivalue(popular,"SOURCE") 
        
        reach = get_xmlnode(SD,"REACH")
        if len(reach) > 0:
            result_dict["reach_rank"] = get_attrivalue(reach[0],"RANK")

        rank = get_xmlnode(SD,"RANK")
        if len(rank) > 0:
            result_dict["rank_delta"] = get_attrivalue(rank[0],"DELTA")

        countrys = get_xmlnode(SD,"COUNTRY")
        if len(countrys) > 0:
            country = countrys[0]
            
            result_dict["country_code"] = get_attrivalue(country,"CODE")
            result_dict["country_name"] = get_attrivalue(country,"NAME")
            result_dict["country_rank"] = get_attrivalue(country,"RANK")

    return result_dict

def run_file(files,filename):

    result_file = open("result","w+")
    output = open(filename,"w+")
    for url in files:
        result = get_xml_data(url,output)
        if result:
            result_str=""
            res_url = "url : %s" %url
            fg = "==================================\n"
            for key in result.keys():
                result_str = result_str + "%20s : %s\n"%(key,result.get(key))

            result_file.write(res_url + fg + result_str + fg + "\n")
        else:
            res_url = "url : %s\n" %url
            result_file.write(res_url + "UNKNOWN\n")
    

    output.close()
    result_file.close()

if __name__ == '__main__':
    #url = "http://data.alexa.com/data?dat=snbamz$cli=10&urli="+sys.argv[1]
    #url="http://data.alexa.com/data?dat=snbamz&cli=10&url=www.google.com"
    #length = len(sys.argv)
    #for i in range(length):
    #    pass
    #url=sys.argv[1]
    file1="/yundisk/luopan/rank/output1"
    file2="/yundisk/luopan/rank/output2"
    i=0
    files = open("top-1m.csv","r")
    run_file(files,file1)
    while True:
        if i%2 == 0:
            if len(open(file1).read())!=0:
                run_file(file1,file2)
                os.remove(file1)
            else:
                break
        else:
            if len(open(file2).read())!=0:
                run_file(file2,file1)
                os.remove(file2)
            else:
                break
        i=i+1
