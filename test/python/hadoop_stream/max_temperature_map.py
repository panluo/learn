#!/usr/bin/env python
import re
import sys

for line in sys.stdin:
    val = line.strip()
    (year,temp,q) = (val[14:18],val[103:108],val[65:66])
    if(temp != "+9999" and re.match("[01459]",q)):
        print "%s\t%s"%(year,temp)
