#!/usr/bin/expect
set timeout 50
set site 180.153.42.72
set user bilinhadoop
set password hadoop0987
spawn ssh -l $user $site
expect "$user@$site's password:"
send "$password\r"
interact
