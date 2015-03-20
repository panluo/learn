#!/bin/sh
expect -c "spawn scp mysql.sh luopan@183.56.131.131:/home/luopan
set timeout 30
expect \"luopan@183.56.131.131's password:\"
set timeout 20
send \"luopan\r\"
set timeout 30
send \"exit\r\"
interact
"


#send "exit\r"
