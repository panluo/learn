#!/usr/bin/env python
import datetime
import MySQLdb

class translate:
    properties = ["time","client_ip","bilin_user_id","win_price","ad_exchange","bid_price","campaign_id","line_item_id","creative_id","size","url","domain","os","browser","site_category","exchange_user_id","user_ip","language","time_zone","geo_info"]
    ip_list = []
    ip_dict = {}
    cursor = None 
    conn = None

    def __init__(self):
        self.conn = MySQLdb.Connect(host='183.56.131.131',user='mataotao',passwd='mataotao',db='cms_development')
        self.cursor = self.conn.cursor()
        self.ip_dict, self.ip_list = self.loadGeoInfo()
        pass

    def __del__(self):
        self.cursor.close()
        self.conn.close()
        pass

    def transTime(self,line_list):
        index_time = properties.index("time")
        time_zone = line_list[properties.index("time_zone")]
        if time_zone is None:
            time_zone = "8"

        time = long(line_list[index_time]) - (int(time_zone)+5)*3600
        new_time = datetime.datetime.strftime(datetime.datetime.fromtimestamp(time),'%Y-%m-%d %H:%M')
        line_list[index_time] = new_time
        return line_list

    def transGeo(self,line_list):
        index_geo = properties.index("geo_info")
        if line_list[index_geo] is None:
            return line_list

        geo = line_list.pop()
        geo_info = geo.split("|")
        i=len(geo_info)
        while i>0:
            i-=1
            line_list.insert(properties.index("user_ip"),geo_info[i])

        return line_list

    def transIpToGeo(self,ip):
        #ip = line_list[properties.index("user_ip")]
        ip_long = self.IpToLong(ip)
        key = self.searchIp(ip_long)
        if key is None:
            return ""
        else:
            ip_code = self.ip_dict.get(key,"")
            print ip_code
            if ip_code is not None:
                self.cursor.execute("select country,region,city from geo_def where geo_code=%s"%ip_code)
            result_geo = self.cursor.fetchone()
	    print result_geo
            return result_geo[0]+"|"+result_geo[1]+"|"+result_geo[2]+"|"
    
    def loadGeoInfo(self):
        file_obj = open("conf/china.csv",'r')
	ip_list = []
	ip_dict = {}
        try:
            for line in file_obj:
                Geo_list = line.split(",")
                ip_from = self.IpToLong(Geo_list[0])
                ip_to = self.IpToLong(Geo_list[1])+1
                if len(ip_list) == 0 or ip_list[len(ip_list)-1] != ip_from:
                    ip_list.append(ip_from)
                
                ip_list.append(ip_to)
                key = str(ip_from) + "_" + str(ip_to)
                ip_dict[key] = Geo_list[2]

            return ip_dict,ip_list

        finally:
            file_obj.close()

    def IpToLong(self,ip):
        result = 0
        nums = ip.split(".")
        result += long(nums[0])<<24
        result += long(nums[1])<<16
        result += long(nums[2])<<8
        result += long(nums[3])
        return result

    def searchIp(self,ip_long):
	ip_list = self.ip_list
        start = 0
        end = len(ip_list)-1
        while end>=start:
            mid = (start+end)/2

            if ip_long >= ip_list[mid] and ip_long < ip_list[mid+1]:
                return str(ip_list[mid])+"_"+str(ip_list[mid+1])
            elif ip_long < ip_list[mid]:
                end = mid - 1
            elif ip_long > ip_list[mid+1]:
                start = mid + 1

        return None


if __name__=="__main__":
    aa = translate()
    print aa.searchIp(aa.transIpToGeo("1.0.1.0"))
