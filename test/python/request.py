#!/usr/bin/env python

# readme #230
# @author LuoPan
# @Date 2014-09-24

import datetime
import MySQLdb
import os
import shutil

#suffix name
suffix_frq=".frq-r-00000"
suffix_cnt=".cnt-r-00000"
days=4
#path
filepath="/yundisk/workspace/result/request_freq/"
#items
#items=['user_ip','language','size','domain','site_category','user_category','page_type','user_gender','page_vertical','ad_slot_visibility','creative_type','referer']
items=['language','size','domain','site_category','page_type','user_gender','creative_type']


def connection():
	conn = MySQLdb.Connect(host="localhost", user="bilinhadoop",passwd="bilin2014",db="test")
	cursor = conn.cursor();
	cursor.execute("create temporary table testing(value varchar(50) primary key,val1 int(20) default 0, val2 int(20) default 0, val3 int(20) default 0, val4 int(20) default 0, val5 int(20) default 0, val6 int(20) default 0, val7 int(20) default 0)")

	return cursor

def getDate():
	date_simply=[];
	today=datetime.date.today()
	oneday=datetime.timedelta(days=1)
	for i in range(days):
		date_simply.append((today-(days-i)*(oneday)).strftime('%Y%m%d'))

	return date_simply

def reader(filename,curs,count,item):
	filename_frq=filename+suffix_frq
	filename_cnt=filename+suffix_cnt
	if os.path.exists(filename_frq) and os.path.exists(filename_cnt):
		print "reading file : %s.*"%filename
		try:
			reader_frq=open(filename_frq,"r")
			reader_cnt=open(filename_cnt,"r")
			cnt=reader_cnt.readline().strip().split("\t")
			i=0
			for eachline in reader_frq:
				frq=eachline.strip().split("\t")
				curs.execute("insert into testing(value,val%s) values('%s',%s) on duplicate key update val%s=%s"%(count,frq[0],frq[1],count,frq[1]))
		#		print "insert into testing(value,val%s) values('%s',%s) on duplicate key update val%s=%s"%(count,frq[0],frq[1],count,frq[1])
				i=i+1
				if i>10:
					break

			curs.execute("insert into testing(value,val%s) values('%s',%s) on duplicate key update val%s=%s"%(count,item,cnt[1],count,cnt[1]))
		#	print "insert into testing(value,val%s) values('%s',%s) on duplicate key update val%s=%s"%(count,item,cnt[1],count,cnt[1])
			reader_cnt.close()
			reader_frq.close()

		except:
			print "Error happend when opening file : %s"%filename

	else:
		print "file %s not exits"%filename

if __name__=="__main__":
	curs = connection()
	dates=getDate() 
	today=datetime.date.today().strftime('%Y%m%d')

	for item in items:
		i=1
		for date in dates:
			filename=filepath+date+"/"+item+"."+date
			reader(filename,curs,i,item)
			i=i+1
		
		outputpath="/yundisk/workspace/result/request/weekly/%s/%s"%(today,item)
		if os.path.exists(outputpath):
			os.remove(outputpath)
		
		curs.execute("select * from testing into outfile '/tmp/%s.tmp'"%item)
#		os.remove("/tmp/rest.txt")
		shutil.copy("/tmp/%s.tmp"%item,outputpath)
		curs.execute("truncate table testing")

	curs.close()
