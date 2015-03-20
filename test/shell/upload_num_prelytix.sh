#!/bin/bash
yest=`date -d yesterday +%Y%m%d`
filepath_HDFS="/user/hadoop/prelytix_num_count/"
filepath_LOCAL="/mnt/luopan/tmp/"
if [ ! -d $filepath_LOCAL ];then
    mkdir $filepath_LOCAL
fi
function downloads(){
    if [ -e $filepath_LOCAL/$yest ];then
        rm $filepath_LOCAL/$yest
    fi
    hadoop fs -get $filepath_HDFS/$yest $filepath_LOCAL
    if [ $? ];then
        awk '{imp=imp+$8;req=req+$12;input=input+$16}END{print "total : '$yest'\timp count : "imp"\treq count : "req"\tinput count : "input}' $filepath_LOCAL/$yest | cat - >> $filepath_LOCAL/$yest
    fi
}

function upload(){
    aws s3 cp $filepath_LOCAL/$yest s3://bilin-data/hadoop/result/prelytix/ \
        && rm $filepath_LOCAL/$yest
}

downloads
