import sys
import random
import httplib
import hashlib
import time
from lib import tanx_bidding_pb2
from utils import Monitor


ROTOCOL_VERSION = 3
CONTENT_TYPE = 'application/octet-stream'
CONTENT_TYPE_HEADER = 'Content-type'
TANXBID_NORMAL_MSG = u'Tanx bid handler normal'

class TanxMonitor(Monitor):

	def __init__(self, *args, **kwargs):
		self.host = kwargs['host']
		self.cls = kwargs['class']
		self.port = kwargs['port']
		self.name = kwargs['name']
		self.url = kwargs['url']
		self.last_error_msg = '%s : %s %s' %(TANXBID_NORMAL_MSG, self.url, self.name)

	#Tanx ping request,bring up from the analogy from tanx
	def GeneratePingRequest(self):
		bid_request = tanx_bidding_pb2.BidRequest()
		bid_request.version = ROTOCOL_VERSION
		bid_request.bid = self._GenerateBid()
		bid_request.is_ping = True
		return bid_request

	def _GenerateBid(self):
		now_Time = str(time.time())[0:65535]
		return hashlib.md5(now_Time+'Tanx').hexdigest()
	
	#Send ping request to check bid module of tanx
	def check(self):
		#serialize to string with the function of tanx_bidding_pb2 
		payload = self.GeneratePingRequest().SerializeToString()
		self.last_error_msg = '%s : %s %s' %(TANXBID_NORMAL_MSG, self.url, self.name)
		# make connection 
		try:
			conn = httplib.HTTPConnection(self.host, self.port)
			conn.request('POST', self.url, payload,
					{CONTENT_TYPE_HEADER: CONTENT_TYPE})
			response = conn.getresponse()
			status = response.status
			conn.close()

		except Exception, e:
			self.last_error_msg = ' Failed to fulfill the request, %s %s' %(e,self.name)
			return 1
		
		if status == 200 or status == 304:
			return 0
		else:
			self.last_error_msg = ' Get error response , code : %s %s' %(status, self.name)
			return 2

	#make the function called as a attribute
	@property
	def last_error(self):
		return self.last_error_msg
