from utils import Monitor
import socket
import urllib2

NGINX_NORMAL_MSG = u'Nginx server normal!'

class NginxMonitor(Monitor):

	def __init__(self, *args, **kwargs):
		self.cls = kwargs['class']
		self.url = kwargs['url']
		self.name = kwargs['name']
		self.last_error_msg = '%s : %s' %(NGINX_NORMAL_MSG, self.name)

	def check(self):

		self.last_error_msg = '%s : %s' %(NGINX_NORMAL_MSG, self.name)
		timeout=10
		socket.setdefaulttimeout(timeout)
		req = urllib2.Request(self.url)
		try:
			res = urllib2.urlopen(req)
		except urllib2.URLError,e:
			if hasattr(e,'reason'):
				self.last_error_msg = 'Failed to reach the server , Reason : %s %s' %(e.reason, self.name)
				return 1
			elif hasattr(e,'code'):
				self.last_error_msg = 'Failed to fulfill the request , Error code : %s %s' %(e.code , self.name)
				return 2
		return 0

	@property
	def last_error(self):
		return self.last_error_msg
