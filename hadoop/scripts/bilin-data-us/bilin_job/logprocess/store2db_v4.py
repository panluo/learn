#!/usr/bin/python
#coding=utf-8
#automatic store data(from hadoop) to database
import os
import tornado.options
import torndb
import time
import datetime
import sys
import tld
from tornado.options import define, options

#mysql database
define("host", default = "127.0.0.1:3306", help = "")
define("rs_database", default = "rs_data", help = "")
define("rs_user", default = "root", help = "")
define("rs_password", default = "", help = "")

class StoreData:
    """
    the function logic is obvious, but the program logic is a little bit messy.
    """
    file_path = sys.argv[2] + "/last/"
    file_success = "success"
    this_hour=sys.argv[1]
    def __init__(self):
        self.rs_db = torndb.Connection(
            host = '127.0.0.1', database = options.rs_database,
            user = options.rs_user, password = options.rs_password)

    def convert_to_datetime(self, hours):
        """
        convert hours to real time.
        """
        return datetime.datetime.fromtimestamp(((int)(hours) - 5) * 3600)
    
    def get_top_domain(self, domain):
        "get top level domain using tld module"

        prefix = "http://www."
        suffix = ['.so','.ng','.pm','.sa','.wf','.kp','.sx']

        url = prefix + domain.strip().strip('.')
        top_domain = domain

        try:
            top_domain = tld.get_tld(url)
        except tld.exceptions.TldDomainNotFound,e:
            isd = False
            for sfix in suffix:
                if domain.find(sfix) >= 0:
                    top_domain = domain[domain.rfind('.',0,len(domain)-4) + 1:]
                    isd = True
                    break

            if isd == False:
                print "Warning: get top domain wrong",domain
                top_domain = domain.strip().strip('.')
        except Exception,e:
            print "Warning: get top domain wrong",domain
            top_domain = domain.strip().strip('.')

        return top_domain

    def run(self):
        """
        first insert data to database,
        then update some field.
        """
        noa = {"bid": "bids", "clk": "clicks", "imp": "impressions", "cvs": "conversions", "win": "wins"}
        sep = '|'
        #dimension = ["lineitem", "domain", "pagetype", "os", "browser", "device", "geo"]

        table_field = ["campaign_id", "lineitem_id", "exchange", 
                       "dimension_value", "time", "bids", "cpm", "impressions", 
                       "ecpm", "clicks", "conversions", "users", "costs", "wins","country"]
        sql = ''     
        try:
            for files in open(self.file_path + self.file_success):
                files = files.strip()
                file_prefix = files[:3]
                if file_prefix == "bid":
                    #insert
                    for line in open(self.file_path + files):
                        record = line.strip().split('\t')

                        #if this is last hour record, add to last hour
                        data = record[0].split(sep)
                        if data[5] != self.this_hour:
                            data_tuple = (data[0], (int)(record[1]), (float)(record[2]), int(data[2]), (int)(data[3]),
                                    data[1], data[4], self.convert_to_datetime(data[5]))

                            sql = """update %s set bids=bids+%d,cpm=cpm+%f where campaign_id=%d and lineitem_id=%d 
                            and exchange='%s' and dimension_value='%s' and time='%s' """ % data_tuple
                            self.rs_db.execute(sql)
                            continue

                        data_tuple = [data[0],table_field[0], table_field[1], 
                                      table_field[2], table_field[3], table_field[4],
                                      table_field[5], table_field[6],
                                      (int)(data[2]), (int)(data[3]), data[1], data[4], 
                                      self.convert_to_datetime(data[5]), (int)(record[1]), (float)(record[2])]
			
                        if data[0] == 'domain':
                            data_tuple += [self.get_top_domain(data[4])]
                            sql = 'insert into %s (%s, %s, %s, %s, %s, %s, %s, top_domain) values (%d, %d, "%s", "%s", "%s", %d, %f, "%s")' % tuple(data_tuple)
                            self.rs_db.execute(sql)
                        else:
                            sql = 'insert into %s (%s, %s, %s, %s, %s, %s, %s) values (%d, %d, "%s", "%s", "%s", %d, %f)' % tuple(data_tuple)
                            self.rs_db.execute(sql)
                
                elif file_prefix == "imp" or file_prefix == "win":
                    #int or float?
                    for line in open(self.file_path + files):
                        record = line.strip().split('\t')
                        data = record[0].split(sep)
                        if data[5] != self.this_hour:
                            data_tuple = (data[0], (int)(record[1]), (float)(record[2]), int(data[2]), (int)(data[3]),
                                    data[1], data[4], self.convert_to_datetime(data[5]))

                            sql = """update %s set impressions=impressions+%d,costs=costs+%f where campaign_id=%d and lineitem_id=%d 
                            and exchange='%s' and dimension_value='%s' and time='%s' """ % data_tuple
                            self.rs_db.execute(sql)
                            continue

                        data_tuple = (data[0],int(record[1]), (float)(record[2]), int(record[3]), int(data[2]), int(data[3]), data[1], data[4], self.convert_to_datetime(data[5]))
                        sql = 'update %s set impressions=%d, costs=%f, users=%d where campaign_id=%d and lineitem_id=%d and exchange="%s" and dimension_value="%s" and time="%s"' % data_tuple
                        self.rs_db.execute(sql)
                
                else:
                    #update some field
                    for line in open(self.file_path + files):
                        record = line.strip().split('\t')
                        data = record[0].split(sep)
                        if data[5] != self.this_hour:
                            data_tuple = (data[0], noa[file_prefix], noa[file_prefix], (int)(record[1]), int(data[2]), (int)(data[3]),
                                    data[1], data[4], self.convert_to_datetime(data[5]))

                            sql = """update %s set %s=%s+%d where campaign_id=%d and lineitem_id=%d 
                            and exchange='%s' and dimension_value='%s' and time='%s' """ % data_tuple
                            self.rs_db.execute(sql)
                            continue

                        data_tuple = (data[0],noa[file_prefix], (int)(record[1]), int(data[2]), int(data[3]), data[1], data[4], self.convert_to_datetime(data[5]))
                        sql = 'update %s set %s=%d where campaign_id=%d and lineitem_id=%d and exchange="%s" and dimension_value="%s" and time="%s"' % data_tuple
                        self.rs_db.execute(sql)

        except Exception, e:
            print("%s%s" % ("ERROR: ", str(e)))
            print(sql)
            exit(1)
    
    
if __name__ == "__main__":
    S = StoreData()
    S.run()

