#!/usr/bin/env python
import os
import MySQLdb
import datetime
import re
import sys

class update_to_db:

    input_file = None
    output_file = None
    black_list = []
    def __init__(self,filename_path="/yundisk/luopan/url/output/input",output_path="/yundisk/luopan/url/result",black_list_path="/yundisk/luopan/url/black_list"):
        try:
            update_to_db.input_file = open(filename_path,"r")
            update_to_db.output_file = open(output_path,"w")
        except Exception,e:
            print "url file not exists"
            print e
            sys.exit(0)

        self.load_black_list(black_list_path)
        try:
            self.conn = MySQLdb.connect(host="localhost",user="root",passwd="",db="test")
            self.cursor = self.conn.cursor()
        except:
            print "connection error"
            sys.exit(0)

    

    def load_black_list(self, filepath):
        openfile = None
        try:
            openfile = open(filepath)
            for key_word in openfile:
                if key_word in update_to_db.black_list:
                    pass
                else:
                    update_to_db.black_list.append(key_word)

        except Exception,e:
            print "black list file not exists"
            print e
        if openfile is not None:
            openfile.close()

    def update(self,time_date):
        yesterday=time_date
        count_file = 0
        count_black = 0
        count_same_key = 0
        create_table="""create table if not exists url
            (
                url varchar(300) not null,
                date datetime not null,
                primary key(url)
           )"""

        #self.cursor.execute(create_table)

        lines=[]
        for line in update_to_db.input_file:
            count_file += 1
            url = line.split("\t")[0]
            if len(url) > 300:
                continue
            domain = self.get_domain(url)
            sign = 0
            for bl in update_to_db.black_list:
                if domain.find(bl.strip()) == 0 or domain.isdigit():
                    sign = 1
                    count_black += 1
                    break

            if sign == 0:
                #lines.append([url,yesterday])
                #if len(lines) > 5000:
                try:
                    self.cursor.executemany("insert into url(url,date) values (%s,%s)",lines)
                    self.conn.commit()
                except Exception,e:
                    print e 
                    count_same_key += 1
                    pass
                        #self.conn.rollback()

    #                lines=[]

       #if len(lines) > 0:
      #      try:
      #          self.cursor.executemany("insert into url(url,date) values (%s,%s)",lines)
      #          self.conn.commit()
      #      except Exception,e:
      #          print e
      #          pass

      #  self.conn.commit()
        
        try:
            self.cursor.execute("select url from url where date(date) = %s" %yesterday)
            for url in self.cursor.fetchall():
                update_to_db.output_file.write(url[0]+"\n")

        except Exception,e:
            print e
            count_same_key += 1
            #self.conn.rollback()
        print count_file
        print count_same_key
        print count_black
    def get_domain(self,url):
        
        extractPattern = r'\w+://([W]{3}.)?(.+\?|.+/|.+\&|.+)'
        try:
            pattern = re.compile(extractPattern,re.IGNORECASE)
            m = pattern.match(url)
            
            return m.group(2) if m else url
        except Exception,e:
            print e
            return ""

    def __del__(self):
        if self.__class__.input_file is not None:
            self.__class__.input_file.close()
        if self.__class__.output_file is not None:
            self.__class__.output_file.close()
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

if __name__ == "__main__":

    time_date = datetime.date.today() - datetime.timedelta(days=1)
    if len(sys.argv) < 3:
        a = update_to_db()
    else:
        a = update_to_db(sys.argv[1],sys.argv[2],sys.argv[3])
        time_date = sys.argv[4]
    a.update(time_date)
