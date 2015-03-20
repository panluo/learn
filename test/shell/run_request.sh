#!/bin/bash
python request.py 
cd /tmp/
expect -c "spawn bash -c \"sudo rm /tmp/*.tmp\"
expect \"*password*:\"
set timeout 100
send \"hadoop0987\n\"

expect eof
"
