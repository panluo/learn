#!/bin/bash
user="mark:x:0:0:this is a test user:/var/mark:nologin"
i=1
while((1==1))
do
    split=`echo $user|cut -d ":" -f$i`
    if [ "$split" != "" ]
    then
        ((i++))
        echo $split
    else
        break
    fi
done
