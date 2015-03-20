#!/usr/bin/python

import os
import torndb
import datetime
import tld
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

host = "127.0.0.1:3306"
user = "root"
password = ""
data_db_name = "rs_data"
cms_db_name = "cms_production"
file_path = sys.argv[2]

class DataReport:

	campaign_header = '''Date,Advertiser,Campaign name,Campaign,Line Item name,Line Item,Creative name,Creative,Ad Size,Impressions,Uniques,Clicks,Cost USD,Cost CNY,Conversions'''
	domain_header = 'Date,Advertiser,Campaign name,Campaign,Line Item name,Line Item,Domain,Impressions,Uniques,Clicks,Cost USD,Cost CNY,Conversions'
	it_info = {}
	def __init__(self):
		pass

	def connect_cms_db(self):
		"connect to cms database"
		self.cms_db = torndb.Connection(host, cms_db_name, user, password)

	def connect_data_db(self):
		"connect to data database"
		self.data_db = torndb.Connection(host, data_db_name, user, password)

	def get_info_by_lineitem(self, lineitem_id):
		"get info by the given lineitem_id"

		sql = '''select ad.lite_name adname,cp.name cname,cp.id cid,it.name itname,it.id itid from 
			lineitem it, campaign cp, advertiser ad where it.id = %d and it.campaign_id = cp.id
			 and cp.advertiser_id = ad.id ''' % lineitem_id

		return self.cms_db.get(sql)

	def get_creative_info(self, creative_id):
		"get creative name and size"

		sql = "select name,width,height from creative where id = %d" % creative_id

		return self.cms_db.get(sql)
		

	def get_creative_data(self, daytime):
		"get creative data of the time"

		sql = '''select dimension_value creative,lineitem_id,sum(impressions) impressions,
			sum(clicks) clicks,sum(costs) costs,sum(conversions) conversions, country,campaign_id cid 
			from creative where date(time) = %s group by dimension_value,lineitem_id order by cid,
			lineitem_id,creative'''
		return self.data_db.query(sql,daytime)


	def creative_report(self, daytime):
		"write creative data to file"

		self.connect_data_db()
		creative_data = self.get_creative_data(daytime)
		self.data_db.close()

		self.connect_cms_db()
		creative_file_name = 'campaign_' + str(daytime)
		creative_file = open( file_path + '/' + creative_file_name + '.csv','w')
		creative_file.write(self.campaign_header + '\n')

		for cdata in creative_data:
			creative_info = self.get_creative_info(int(cdata['creative']))
			info = self.get_info_by_lineitem(int(cdata['lineitem_id']))
			if info['adname'] == 'Bilin':
				continue
			data_row = [daytime,info['adname'],info['cname'],str(info['cid']),info['itname'],
					str(info['itid'])]

			self.it_info[info['itid']] = ','.join(data_row)

			data_row += [creative_info['name'],str(cdata['creative']),
					str(creative_info['width']) + "*" + str(creative_info['height']),
					str(cdata['impressions']),'',str(cdata['clicks'])]

			if cdata['country'] == 'CN':
				data_row += ['',str(round(cdata['costs']/100000.0,4)),str(cdata['conversions'])]
			else:
				data_row += [str(round(cdata['costs']/1000,4)),'',str(cdata['conversions'])]
			
			creative_file.write(','.join(data_row) + '\n')
		
		creative_file.close()
		self.cms_db.close()

	def domain_report(self, daytime):
		"write domain data to file"
		
		domain_file_name = 'domain_' + str(daytime)
		domain_file = open(file_path + '/' + domain_file_name + '.csv', 'w')
		domain_file.write(self.domain_header + '\n')

		self.connect_data_db()

		for lineitem in self.it_info.keys():
			sql = ''' select top_domain, sum(impressions) impressions, sum(clicks) clicks, 
				sum(costs) costs, sum(conversions) conversions, country from domain
				where date(time) = '%s' and lineitem_id = %d and 
				impressions != 0  group by top_domain; ''' % (daytime, lineitem)
			domain_data = self.data_db.query(sql)
			for dom in domain_data:
				top_domain = ''
				if dom['top_domain'] == None:
					top_domain = 'unknown' 
				else:
					top_domain = dom['top_domain']

				data_row = [top_domain, str(dom['impressions']),'', str(dom['clicks'])]

				if dom['country'] == 'CN':
					data_row += ['', str(round(dom['costs']/100000.0,4)),str(dom['conversions'])]
				else:
					data_row += [str(round(dom['costs']/1000,4)),'',str(dom['conversions'])]
				#print(self.it_info[lineitem])
				#print(data_row)
				domain_file.write(self.it_info[lineitem] + ',' + ','.join(data_row) + '\n')

		domain_file.close()
		self.data_db.close()

if __name__ == "__main__":
	dr = DataReport()
	daytime = sys.argv[1]
	dr.creative_report(daytime)
	dr.domain_report(daytime)
