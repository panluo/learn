#!/usr/bin/expect
set password [lindex $argv 2]
set files [lindex $argv 3]

if {$files=="true"} {
    spawn scp -r [lindex $argv 0] [lindex $argv 1]
} else {
    spawn scp [lindex $argv 0] [lindex $argv 1] 
} 
set timeout 100
expect { 
    "(yes/no)?" {
        send "yes\n"
        set timeout 100
        expect "password:"
        send "${password}\r"
    }

    "password:" {
        send "${password}\r"                                            
    }
}

interact
