
import sys
import random
import string
import httplib
from lib import baidu_realtime_bidding_pb2
from utils import Monitor


CONTENT_TYPE = 'application/octet-stream'
CONTENT_TYPE_HEADER = 'Content-type'
BESBID_NORMAL_MSG = u'Bes bid handler normal'

class BesMonitor(Monitor):

	def __init__(self,*args,**kwargs):
		self.host = kwargs['host']
		self.cls = kwargs['class']
		self.port = kwargs['port']
		self.name = kwargs['name']
		self.url = kwargs['url']
		self.last_error_msg = '%s : %s %s' %(BESBID_NORMAL_MSG, self.url, self.name)

	# Baidu ping request,bring up from the analogy from baidu
	def GeneratePingRequest(self):
		bid_request = baidu_realtime_bidding_pb2.BidRequest()
		bid_request.id = self._GenerateId(32)
		bid_request.is_ping = True
		return bid_request

	#just generate a ID
	def _GenerateId(self,length):
		allow_list = string.ascii_lowercase + string.digits
		id = [ random.choice(allow_list) for _ in range(length) ]
		return "".join(id)

	# send ping request to check bid module of baidu
	def check(self):
		#serialize to string with the function of tanx_bidding_pb2
		payload = self.GeneratePingRequest().SerializeToString()
		self.last_error_msg = '%s : %s %s' %(BESBID_NORMAL_MSG, self.url, self.name)
		
		# make connection to server with the post method
		try:
			conn = httplib.HTTPConnection(self.host,self.port)
			conn.request('POST', self.url, payload, 
				{CONTENT_TYPE_HEADER: CONTENT_TYPE})
		
			response = conn.getresponse()
			status = response.status
			conn.close()

		except Exception,e:
			self.last_error_msg = ' Failed to fulfill the request : %s %s' %(e,self.name)
			return 1
		
		if status == 200 or status == 304:
			return 0
		else:
			self.last_error_msg = ' Get error response , code : %s %s' %(status,self.name) 
			return 2

	#make the function called as a attribute
	@property
	def last_error(self):
		return self.last_error_msg
