#!/usr/bin/enc python
#coding=utf-8
#report system api
import tornado.ioloop
import tornado.web
import tornado.options
import tornado.httpserver
import tornado.template
import os.path
import torndb
import sys
import json
import decimal
import datetime
import time

from tornado.options import define, options

define("port", default = 8800, help = "", type = int)
define("host", default = "127.0.0.1:3306", help = "")

#report system database
define("rs_database", default = "report_system", help = "")
define("rs_user", default = "root", help = "")
define("rs_password", default = "", help = "")

#cms database
define("cms_database", default = "db_cms", help = "")
define("cms_user", default = "root", help = "")
define("cms_password", default = "", help = "")
class TimeUtil():
    @staticmethod
    def datetime_to_timestamp(t):
        return (int)(time.mktime(time.strptime(str(t), '%Y-%m-%d %H:%M:%S')))
    
    @staticmethod
    def date_to_timestamp(t):
        return (int)(time.mktime(time.strptime(str(t), '%Y-%m-%d')))
    
    @staticmethod
    def timestamp_to_date(t):
        return datetime.date.fromtimestamp(t)
    
    @staticmethod
    def timestamp_to_datetime(t):
        return datetime.datetime.fromtimestamp(t)    

    @staticmethod
    def diff_date(t1, t2):
        if isinstance(t1, datetime.date) and isinstance(t2, datetime.date):
            return (t2 - t1).days
        return None
    
    @staticmethod
    def datetime_to_date(t):
        if isinstance(t, datetime.datetime):
            return datetime.date(t.year, t.month, t.day)
        return None

class JsonEncoder(json.JSONEncoder, TimeUtil):
    """
    convert decimal type to int, sum in sql sentence may 
    cause type change to decimal
    """
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return (int)(obj)
        elif isinstance(obj, datetime.datetime):
            return TimeUtil.datetime_to_timestamp(obj)
        elif isinstance(obj, datetime.date):
            return TimeUtil.date_to_timestamp(obj)
        else:
            return json.JSONEncoder.default(self, obj)

class Application(tornado.web.Application):
    def __init__(self):
        settings = dict(
            template_path = os.path.join(os.path.dirname(__file__), "template"),
            static_path = os.path.join(os.path.dirname(__file__), "static"),
        )
        
        handlers = [
            (r"/", HomeHandler),
            (r"/fetch", FetchHandler),
            (r"/query", QueryHandler),
            (r"/start", StartHandler),
        ]        
        
        tornado.web.Application.__init__(self, handlers, **settings)
        
        self.rs_db = torndb.Connection(
            host = options.host, database = options.rs_database,
            user = options.rs_user, password = options.rs_password)
        
        self.cms_db = torndb.Connection(
            host = options.host, database = options.cms_database,
            user = options.cms_user, password = options.cms_password)

class BaseHandler(tornado.web.RequestHandler, TimeUtil):
    arg_list = ['type', 'value', 'quota', 'from', 'to', 'granu']
    
    arg_default = {'type': 'campaign', 'value': None, 'quota': 'bid', 'from': time.time(), 'to': time.time(), 'granu': 'day'}
    
    type_map = {"campaign": "campaign_id", "line_item": "lineitem_id",
                "creative": "creative", "bid": "bids", "imp": "impressions", 
                "clk": "clicks", "con": "conversions", "user": "users"}
    
    @property
    def rs_db(self):
        return self.application.rs_db

    @property
    def cms_db(self):
        return self.application.cms_db
    
    def query(self, string, db):
        try:
            data = db.query(string)
            if not data:
                return None
            else:
                return data
        except Exception,  e:
            print(str(e))
            return None
    
    def repack(self, lst, begin, end, granu):
        """
        if granu is hour,
        then the return value should be filled with 24 hours' data 
        if request for one day's data or two
        """
        ret = []
        if not lst:
            return ret
        
        days = TimeUtil.diff_date(begin, end) + 1
        if not days:
            return None
        keys = []        
        for dct in lst:
            keys = keys + dct.keys()
        
        keys = list(set(keys))
        key_map = dict(zip(keys, [0] * len(keys)))        
        
        if granu == 'day':
            for i in xrange(days):
                tmp = {'time': TimeUtil.timestamp_to_date(TimeUtil.date_to_timestamp(begin) + 3600 * 24 * i)}
                ret.append(dict(key_map.items() + tmp.items()))
                    
            for key in lst:
                day_diff = TimeUtil.diff_date(begin, key['time'])
                ret[day_diff].update(key)            
        else:
            for i in xrange(days):
                for j in xrange(24):
                    tmp = {'time': TimeUtil.timestamp_to_datetime(TimeUtil.date_to_timestamp(begin) + 3600 * j + 3600 * 24 * i)}
                    ret.append(dict(key_map.items() + tmp.items()))
            
            for key in lst:
                tmp = key['time']
                day_diff = TimeUtil.diff_date(begin, TimeUtil.datetime_to_date(tmp))
                index = tmp.hour + day_diff * 24
                ret[index].update(key)
        return ret 
       
    def format(self, data, args):
        """
        change return format from
            {'root':[{'time': 1, 'name': 2, 'bids': 3}, {'time': 1, 'name': 12, 'bids': 13}]}
        to
            {'root':[{'time': 1, '2': 3, '12': 13}]}
        so, the data format then support request contain more than one id(value).
        """
        if not isinstance(data, list):
            return None
        ret = []
        for key in data:
            tmp = {}
            tmp['time'] = key['time']
            tmp[key[self.type_map[args['type']]]] = key[args['quota']]
            ret.append(tmp)
        return ret    

class HomeHandler(BaseHandler):
    def get(self):
        self.render("index.html")

class StartHandler(BaseHandler):
    """
    The request format:
    url/start?id=advertiser_id
    
    The response format:
    {
        'root': [ {'time': 108702000, '5090': 1, '100029': 2}, {'time': 108702000, '5090': 3, '100029': 3} ]
    }
    """
    def get(self):
        ret = { 'root': None }
        try:
            advertiser_id = self.get_argument('id', None)
            if advertiser_id:
                sql = 'select id from campaign where advertiser_id=%d' % (int)(advertiser_id)
                data = self.query(sql, self.cms_db)
                campaign_ids = []
                for key in data:
                    campaign_ids.append(str(key['id']))
                args = { 'type': 'campaign', 'quota': 'bid', 'granu': 'hour' }
                args['value'] = campaign_ids
                
                args['from'] = TimeUtil.timestamp_to_date(1404662400)
                args['to'] = TimeUtil.timestamp_to_date(1404748800)
                
                #args['from'] = time.time() - 3600 * 24
                #args['to'] = time.time()
                format_list = (self.type_map[args['quota']], args['quota'], self.type_map[args['type']], ','.join(campaign_ids), args['from'], args['to'])
                sql = 'select time, "campaigns" as campaign_id, sum(%s) as %s from rs where dimension_type="lineitem" and %s in (%s) and date(time) between "%s" and "%s" group by time order by time' % format_list
                print sql
                data = self.query(sql, self.rs_db)
                print data
                data = self.format(data, args)
                print data
                data = self.repack(data, args['from'], args['to'], args['granu'])
                print data
                ret['root'] = data
                self.write(json.dumps(ret, cls=JsonEncoder))                
            else:
                self.write(json.dumps(ret))
        except Exception, e:
            print str(e)
            self.write(json.dumps(ret))

class FetchHandler(BaseHandler):
    """
    The request format:
    url/fetch?type=(campaign, creative, line_item)&value=[id_list]&quota=(bid|clk|imp|con|win)&from=108702000&to=108702000&granu=(hour|day)
    
    The response format:
    {
        'root': [ {'time': 108702000, '5090': 1, '100029': 2}, {'time': 108702000, '5090': 3, '100029': 3} ]
    }
    """
    def get(self):
        ret = { 'root': None }
        args = self.validate_arg()
        print args
        if not args:
            self.write(json.dumps(ret))
        else:
            if args['granu'] == 'hour':
                format_list = (self.type_map[args['type']], self.type_map[args['quota']], args['quota'], self.type_map[args['type']], ','.join(args['value']), args['from'], args['to'], self.type_map[args['type']])
                sql = 'select time, %s, sum(%s) as %s from rs where dimension_type="lineitem" and %s in (%s) and date(time) between "%s" and "%s" group by %s,time order by time' % format_list
            else:
                format_list = (self.type_map[args['type']], self.type_map[args['quota']], args['quota'], self.type_map[args['type']], ','.join(args['value']), args['from'], args['to'], self.type_map[args['type']])
                sql = 'select date(time) as time, %s, sum(%s) as %s from rs where dimension_type="lineitem" and %s in (%s) and date(time) between "%s" and "%s" group by %s, date(time) order by time' % format_list
            print sql
            data = self.query(sql, self.rs_db)
            print data
            data = self.format(data, args)
            print data
            data = self.repack(data, args['from'], args['to'], args['granu'])
            print data
            ret['root'] = data
            self.write(json.dumps(ret, cls=JsonEncoder))
    
    def validate_arg(self):
        args = {}
        for arg in self.arg_list:
            if arg == 'value':
                tmp = self.get_arguments(arg, self.arg_default[arg])
            else:
                tmp = self.get_argument(arg, self.arg_default[arg])
            if not tmp:
                return None
            if arg in ('from', 'to'):
                tmp = TimeUtil.timestamp_to_date((float)(tmp))            
            args[arg] = tmp
        return args
 
class QueryHandler(BaseHandler):
    """
    The request format:
    url/query?advertiser=(advertiser_id)&type=(campaign, creative, line_item)
    
    The response format:
    {
        'root': [{'id': 1, 'name': 'bilin1'}, {'id': 2, 'name': 'bilin2'}]
    }
    """
    def get(self):
        advertiser_id, type_name = self.get_argument("advertiser", None), self.get_argument("type", None)
        ret = {'root': None}
        if type_name in ('campaign', 'creative'):
            sql = 'select id, name from %s where advertiser_id=%s' % (type_name, advertiser_id)
            ret['root'] = self.query(sql, self.cms_db)
        elif type_name == 'line_item':
            sql = 'select id from campaign where advertiser_id=%s' % advertiser_id
            campaign_list = self.query(sql, self.cms_db)
            if campaign_list:
                data = []
                for key in campaign_list:
                    sql = 'select id, name from line_item where campaign_id=%s' % key["id"]
                    lineitems = self.query(sql, self.cms_db)
                    if lineitems:
                        data = data + lineitems
                ret['root'] = data
        self.write(json.dumps(ret))
        
    
def main():
    tornado.options.options.logging = "debug"
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)    
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
