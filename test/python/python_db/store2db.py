#!/usr/bin/python
#coding=utf-8
#automatic store data(from hadoop) to database
import os
import tornado.options
import torndb
import time
import datetime
from tornado.options import define, options

#mysql database
define("host", default = "127.0.0.1:3306", help = "")
define("rs_database", default = "report_system", help = "")
define("rs_user", default = "root", help = "")
define("rs_password", default = "", help = "")

class StoreData:
    """
    the function logic is obvious, but the program logic is a little bit messy.
    """
    file_path = "./log/"
    file_success = "success"
    def __init__(self):
        self.rs_db = torndb.Connection(
            host = options.host, database = options.rs_database,
            user = options.rs_user, password = options.rs_password)
    
    def convert_to_datetime(self, hours):
        """
        convert hours to real time.
        """
        return datetime.datetime.fromtimestamp((int)(hours) * 3600)
    
    def run(self):
        """
        first insert data to database,
        then update some field.
        """
        noa = {"bid": "bids", "clk": "clicks", "imp": "impressions", "con": "conversions", "win": "wins"}
        sep = '_'
        #dimension = ["lineitem", "domain", "pagetype", "os", "browser", "device", "geo"]

        table_field = ["mk", "campaign_id", "lineitem_id", "dimension_type", 
                       "dimension_value", "time", "bids", "cpm", "impressions", 
                       "ecpm", "clicks", "conversions", "users", "costs", "wins"]
        try:
            for files in open(self.file_path + self.file_success):
                # remove space before and end of the string
                files = files.strip()
                file_prefix = files.split(sep)[0]
                if file_prefix == "bid":
                    #insert
                    for line in open(self.file_path + files):
                        record = line.split()
                        print record
                        data = record[0].split(sep)
                        print data
                        #if the data length is 4, insert unknown to make 5
                        if len(data) == 4:
                            data[3:3] = ['unknown']
                        data_tuple = (table_field[0], table_field[1], table_field[2], 
                                      table_field[3], table_field[4], table_field[5], 
                                      table_field[6], table_field[7], record[0], 
                                      (int)(data[1]), (int)(data[2]), data[0], data[3], 
                                      self.convert_to_datetime(data[4]), (int)(record[1]), int(float(record[2])))
                        print data_tuple
                        sql = 'insert into rs (%s, %s, %s, %s, %s, %s, %s, %s) values ("%s", %d, %d, "%s", "%s", "%s", %d, %d)' % data_tuple
                        print sql
                        self.rs_db.execute(sql)
                
                elif file_prefix == "win":
                    #int or float?
                    for line in open(self.file_path + files):
                        record = line.split()
                        data_tuple = (noa[file_prefix], (int)(record[1]), table_field[13], int(float(record[2])),
                                      table_field[12], (int)(record[3]), record[0])
                        sql = 'update rs set %s=%d, %s=%d, %s=%d where mk="%s"' % data_tuple
                        print sql
                        self.rs_db.execute(sql)
                    
                else:
                    #update some field
                    for line in open(self.file_path + files):
                        record = line.split()
                        data_tuple = (noa[file_prefix], (int)(record[1]), record[0])
                        sql = 'update rs set %s=%d where mk="%s"' % data_tuple
                        print sql
                        self.rs_db.execute(sql)
        except Exception, e:
            print(str(e))
    
    def __del__(self):
        os.system("rm -f %s%s" % (self.file_path, self.file_success))

if __name__ == "__main__":
    S = StoreData()
    S.run()


