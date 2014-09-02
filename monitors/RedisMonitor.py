from utils import Monitor
import redis

REDIS_NORMAL_MSG = u'Redis server normal!'

class RedisMonitor(Monitor):

    def __init__(self, *args, **kwargs):
	self.cls = kwargs['class']
	self.server = kwargs['server']
	self.port = kwargs['port']
	self.name = kwargs['name']
	self.last_error_msg = '%s : %s %s %s' %(REDIS_NORMAL_MSG, self.server,str(self.port),self.name)


    def check(self):

	self.last_error_msg = '%s : %s %s %s' %(REDIS_NORMAL_MSG, self.server,str(self.port),self.name)
	try:
	    conn = redis.Redis(self.server, int(self.port))
	    status = conn.ping()
	    if not status: 
		self.last_error_msg = u'Redis Ping Error: %s %s %s' %(self.server, str(self.port),self.name)
		return 1
	except:
	    self.last_error_msg = u'Redis Connection Error: %s %s %s' %(self.server, str(self.port),self.name)
	    return 2
	return 0

    @property
    def last_error(self):
	return self.last_error_msg
