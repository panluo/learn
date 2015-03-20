#!/usr/bin/python
#	Store the request log aggregations result to database
# History
#	2014-11-10   mataotao  v1.0.0

import os
import torndb
import datetime
import sys

host = "127.0.0.1:3306"
database = "data"
user = "root"
password = ""
table = "request"

class StoreReqResult:
	
	dir_path = sys.argv[1]
	table_field = ["time","exchange","dimension","attribute","numbers","country"]
	dimensions = ["domain","size","os","browser","site_category","geo","page_vertical","page_type"]
	cn_exchange = ["baidu","tanx"]
	us_exchange = ["bidswitch"]

	def __init__(self):
		"connect database"
		self.db = torndb.Connection(host, database, user, password)


	def cov_date(self, day_of_year):
		"convert day(YYYYMMDD) to real time."
		year,mon,day = int(day_of_year[:4]), int(day_of_year[4:6]), int(day_of_year[6:])
		return datetime.datetime(year,mon,day)

	def get_country(self, exchange):
		"get country code by exchange"
		if exchange in self.cn_exchange:
			return "CN"
		return "US"

	def run(self):
		"insert data into database."
		filename_sep = "."
		line_sep = "\t"
		sql=""
		try:
			files = os.listdir(self.dir_path)
			for file in files:
				name_splits = file.split(filename_sep)
				if name_splits[1] not in self.dimensions:
					continue

				country = self.get_country(name_splits[0])

				if name_splits[0] != "bidswitch":
					
					num = 0
					for line in open(self.dir_path + "/" + file):
						num += 1
						if num > 1000:
							break
						
						fields = line.split(line_sep)

						if fields[0].find('http') == 0:
							continue

						data_tuple = (table,self.cov_date(name_splits[2]), name_splits[0], 
										name_splits[1], fields[0].replace("\'","\\'").replace('%','%%'), int(fields[1]), country)
						sql = """insert into %s (time,exchange,dimension,attribute,numbers,country)
							values ('%s', '%s', '%s', '%s', %d, '%s') """ % data_tuple
						self.db.execute(sql)
				
				else:
					for line in open(self.dir_path + "/" + file):
						fields = line.split(line_sep)

						data_tuple = (table, self.cov_date(name_splits[2]), fields[0],
										name_splits[1], fields[1], int(fields[2]), country)

						sql = """insert into %s (time,exchange,dimension,attribute,numbers,country)
							values ('%s', '%s', '%s', '%s', %d, '%s') """ % data_tuple
						self.db.execute(sql)

		except Exception, e:
			print(sql)
			print("%s%s" % ("ERROR: ",str(e)))
			exit(1)

if __name__ == "__main__":
	srr = StoreReqResult()
	srr.run()
