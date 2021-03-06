#!/usr/bin/env python
#coding=utf-8
######################################
#@author luopan
#@date 2014-09-19
#
# for update table for pricing factor immediately

import MySQLdb
import datetime
def getYesterday():
	today=datetime.date.today()	
	oneday=datetime.timedelta(days=1)
	yesterday=today-oneday
	print "doing log with the date : %s" %yesterday
	return yesterday

def getConnected():

	conn = MySQLdb.Connect(host='localhost',user='root',passwd='',db='report_system')
	print "Connected with mysql"
	return conn

def insert_to_table():

	conn = getConnected()
	cursor = conn.cursor()
	yesterday = getYesterday()
	#for each item,get info for the table of factor
	total_imp = "select campaign_id, dimension_type, dimension_value, sum(impressions), sum(clicks) from report_system.rs where dimension_type!='lineitem' and date(time)='%s' and dimension_type!='pagetype' group by campaign_id,dimension_value" %yesterday

	#print total_imp
	cursor.execute(total_imp)
	result_rs = cursor.fetchall()
	
	#update table
	for i in result_rs:
		tmp="insert into test.factor (campaign_id, variable_name, variable_value, imp_var, imp_var_att) values (%s,'%s','%s',%s,%s) on duplicate key update imp_var=imp_var+%s,imp_var_att=imp_var_att+%s" %(i[0],i[1],i[2],i[3],i[4],i[3],i[4])
		cursor.execute(tmp)

	#for each campaign, get total num of imp and clk
	total_count="select campaign_id,sum(impressions),sum(clicks) from rs where dimension_type='lineitem' and date(time)='%s' group by campaign_id" %yesterday

	cursor.execute(total_count)
	result_rs = cursor.fetchall()
	# update table with each campaign's total count
	for i in result_rs:
		if(i[1]>0):
			update="update test.factor set imp_camp=imp_camp+%s,imp_camp_att=imp_camp_att+%s,probability=imp_var_att/imp_camp_att where campaign_id=%s" %(i[1],i[2],i[0])
			cursor.execute(update)

	#print update
	result_rs=""
	print "updated log of %s" %yesterday
	cursor.close()
        conn.close()

insert_to_table()
