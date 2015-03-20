#!/usr/bin/python
#	Store the pixel log aggregations result to database
# History
#	

import os
import torndb
import datetime
import sys

host = "127.0.0.1:3306"
database = "data"
user = "root"
password = ""
table = "pixel"

class StorePixelResult:
	
	dir_path = sys.argv[1]
	table_field = ["time","pixel_id","dimension_type","dimension_value","fired_num","uniq_user_num","req_num"]
	cn_exchange = ["baidu","tanx"]
	us_exchange = ["bidswitch"]

	def __init__(self):
		"connect database"
		self.db = torndb.Connection(host, database, user, password)


	def convert_to_datetime(self, hours):
		"convert hours to datatime"
		return datetime.datetime.fromtimestamp((int)(hours) * 3600)
	

	def get_country(self, exchange):
		"get country code by exchange"
		if exchange in self.cn_exchange:
			return "CN"
		return "US"

	def run(self):
		"insert data into database."
		field_sep = "\t"
		key_sep = "|"
		sql=""
		try:
			files = os.listdir(self.dir_path)
			for file in files:
				for line in open(self.dir_path + "/" + file):
					records = line.split(field_sep)
					keys = records[0].split(key_sep)

					data_tuple = (table,self.convert_to_datetime(keys[2]), int(keys[1]),
									"pixel", int(keys[1]), int(records[1]), int(records[2]))

					sql = """insert into %s (time,pixel_id,dimension_type,dimension_value,fired_num,uniq_user_num)
						values ('%s', '%d', '%s', '%s', %d, '%d') """ % data_tuple
					self.db.execute(sql)
				
		except Exception, e:
			print(sql)
			print("%s%s" % ("ERROR: ",str(e)))

if __name__ == "__main__":
	s2db = StorePixelResult()
	s2db.run()
