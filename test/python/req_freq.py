#!/usr/bin/env python

# readme #230
# @author LuoPan
# @Date 2014-09-24

import os
import datetime

#suffix name
suffix=".frq-r-00000"
#path
filepath="/yundisk/workspace/result/request_freq/"
#items
items=['user_ip','language','size','domain','site_category','user_category','page_type','user_gender','page_vertical','ad_slot_visibility','creative_type','referer']
for item in items:
    locals()["%s"%item] = [([0]*8) for i in range(1000)]


if __name__ == '__main__':
    today=datetime.date.today()
    oneday=datetime.timedelta(days=1)
    for i in range(7):
        date_simply = (today-(i+1)*(oneday)).strftime('%Y%m%d')
        for item in items:
            filename=filepath+date_simply+"/"+item+"."+date_simply+suffix
            try:
                if os.path.exits(filename):
                    reader = open(filename,"r")

                    for eachline in reader:
                        values = eachline.strip().split("\t")
                        j=0
                        while locals()["%s"%item][j] != 0 or j<len(locals()["%s"%item]):
                            if locals()["%s"%item][j] == values[0]:
                                locals()["%s"%item][j][i+1] = values[1]
                                break
                            j=j+1
                        
                        if j<len(locals()["%s"%item]) and locals()["%s"%item][j]==0:
                            locals()["%s"%item][j][i+1]=values[1]
                            locals()["%s"%item][j][0]=values[0]
                        else:
                            break
                    
                    reader.close()
                else:
                    print "file not exits : %s" %filename

            except:
                print "Error when opening file : %s" %filename
            

for item in items:
#    output = open("%s"%item,"w+")
    print locals()["%s"%item]
#    output.writelines(locals()["%s"%item])
#    output.close()
