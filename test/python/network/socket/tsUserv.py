#!/usr/bin/env python

from socket import *
from time import ctime

HOST = 'localhost'
PORT = 21567
BUFSIZ = 1024
ADDR = (HOST, PORT)

udpSer = socket(AF_INET, SOCK_DGRAM)
udpSer.bind(ADDR)

while True:
    print 'waiting for message ...'

    data, addr = udpSer.recvfrom(BUFSIZ)
    udpSer.sendto('[%s] %s' %(ctime(), data),addr)
    print '...received from and returned to :',addr

udpSer.close()
