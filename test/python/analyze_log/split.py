#!/usr/bin/env python
# analyze bid imp win clk log from bidder,create file for data of table rs
# format of one line of file item|campaign_id|line_item_id|item_value|time  numbers
# @author luopan
# @time 2014-10-09
#
import linecache
import sys
LOGTYPE={'bid':1,'imp':2,'win':3,'clk':4}
def split_log(inputPath,outputPath,items,keyinfo,length):
    fs = open(inputPath)
    out = open(outputPath,"a")
    try:
        for line in fs:
            tmp = line.strip("\n").split("\t")
            if len(tmp)==length+1:
                hours = int(tmp[keyinfo["time"]])/3600 
                for i in items.keys():
                    key = "%s|%s|%s|%s|%s"%(items[i],tmp[keyinfo["campaign_id"]],tmp[keyinfo["lineitem"]],tmp[i+1],hours)
                    if not tmp[keyinfo["exchange_user_id"]].strip():
                            user_id = "unknow"
                    else:
                        user_id = tmp[keyinfo["exchange_user_id"]]

                        out.write("%s\t%s\t%s\t%s\n"%(key,'1',tmp[keyinfo["win_price"]],user_id))

    finally:
        fs.close()
        out.close()

def split_toge(path, outputPath, items, keyinfo, length,logtype):
    fs = open(path)
    try:
        sizehint = 8388600
        position = 0
        lines = fs.readlines(sizehint)
        while not fs.tell() - position <= 0:
            diction = {}
            position = fs.tell()

            for line in lines:
                tmp = line.strip("\n").split("\t")
                if len(tmp) == length+1:
                    hours = int(tmp[keyinfo["time"]])/3600
                    for i in items.keys():
                       # print items[i]+tmp[i+1]+"\t"+"1"+"\n"
                        key = "%s|%s|%s|%s|%s"%(items[i],tmp[keyinfo["campaign_id"]],tmp[keyinfo["lineitem"]],tmp[i+1],hours)

                        if logtype == "bid":
                            bid_price = int(tmp[keyinfo["bid_price"]])
                            if diction.has_key(key):
                                diction[key][0] = diction[key][0] + 1
                                diction[key][1] = diction[key][1] + bid_price
                            else:
                                diction[key] = [1,bid_price]
                        else:   
                            if diction.has_key(key):
                                diction[key] = diction[key] + 1
                            else:
                                diction[key] = 1

            lines=fs.readlines(sizehint)

            out = open(outputPath,"a")
            try:
		if logtype == "bid":
		    for key in diction.keys():
		        out.write("%s\t%s\t%s\n"%(key,diction[key][0],diction[key][1]))
                else:
                    for key in diction.keys():
                        out.write("%s\t%s\n"%(key,diction[key]))
            finally:
                out.close()
    finally:
        fs.close()

def load_conf(logtype):
    path = "/yundisk/luopan/conf.txt"
    theline = linecache.getline(path,LOGTYPE[logtype])
    usefull = linecache.getline(path,5)
#    print theline
    print usefull
    line = theline.strip("\n").split(",")
    useline = usefull.strip("\n").split(",")
    keyinfo = {}
    for i in range(len(line)):
        if line[i] == "campaign_id" or line[i] == "lineitem" or line[i] == "time" or line[i] == "bid_price" or line[i] == "exchange_user_id" or line[i] == "win_price":
            keyinfo[line[i]] = i+1

    dictions = {}
    for i in range(len(useline)):
        for j in range(len(line)):
            if line[j] == useline[i]:
                dictions[j]=line[j]
                print "dictions key=%s,value=%s"%(j,line[j])
                break
    
    return dictions,keyinfo,len(line)

def main(inputPath, outputPath = "/tmp/logProcess_result.txt", logType):
    
    dictions,keyinfo,length = load_conf(logType)
    if logType == "win":
	print "win type"
        split_log(inputPath,outputPath,dictions,keyinfo,length)
    else:
        split_toge(inputPath,outputPath,dictions,keyinfo,length,logType)
#    for key in LOGTYPE.keys():
#        dictions,keyinfo,length = load_conf(key)
#        for path in glob.glob("/home/luo/sample/*"+key+"*"):
#            split_toge(path,dictions,keyinfo,length)

if __name__ == "__main__":
    print "in main"
    inputPath = sys.argv[1]
    outputPath = sys.argv[2]
    logType = sys.argv[3]
    if LOGTPYE.has_key(logType):
        print inputPath+"--"+outputPath+"--"+logType
        main(inputPath, outputPath, logType)
    else:
        print "input element error with logType : %s"%logType

