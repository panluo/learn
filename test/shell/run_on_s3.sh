#!/bin/bash



function run()
{
    days=$1
    hours=`seq 10 23`
    hours="00 01 02 03 04 05 06 07 08 09 $hours"

    for hour in $hours;
    do
        sutc=`date -d "$days $hour" +%s`
        setc=$(($sutc-18000))
        h=`date -d @$setc +%H`
        day=`date -d @$setc +%Y%m%d`
        if [ $day  -gt 20141210 ] && [ $day -lt 20141217 ];then
            aws emr add-steps --cluster-id j-1QV80VNOE43VI \
                --steps Type=CUSTOM_JAR,Name=prelytix_extract,ActionOnFailure=CONTINUE,Jar=s3://bilin-data/hadoop/job/prelytix_extract/RequestExtractUrl-1.0.4.jar,Args=com.bilin.main.Processor,s3://bilin-data/hadoop/data_log/req/$days/*$days$hour*,s3://bilin-data/hadoop/data_log/win/$days/$hour/,/user/hadoop/result/prelytix_extract/$day/$h/,req,s3://bilin-data/hadoop/job/prelytix_extract/extracturl-1.0.4.properties,100
        fi
    done
}

function main(){
    date="20141211 20141212 20141213 20141214 20141215 20141216 20141217"
    for day in $date
    do
        run $day
        echo
    done
}

main
