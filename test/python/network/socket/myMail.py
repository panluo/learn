#!/usr/bin/env python

from smtplib import SMTP
from poplib import POP3
from time import sleep

SMTPSVR = 'smtp.exmail.qq.com'
POP3SVR = 'pop.exmail.qq.com'

origHdrs = ['From: pan.luo@bilintechnology.com','To:
