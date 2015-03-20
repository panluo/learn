#!/bin/bash
#
#

LOCAL_HOME="/mnt/bilin_emr"
LOCAL_RESULT_HOME="$LOCAL_HOME/result"
LOCAL_TMP="$LOCAL_HOME/tmp"

HDFS_HOME="/user/hadoop"
HDFS_RESULT_HOME="$HDFS_HOME/result"


stime=$1
day=${1:0:8}
hour=${1:8:2}


# Copy result from hdfs to local, then uncompress the file and
# extract url information
function proc_result_url_extract(){
	echo "Start extract url"
	local local_result_path="$LOCAL_RESULT_HOME/prelytix_extract/$stime"
	test -d $local_result_path && rm -fr $local_result_path
	mkdir -p $local_result_path

	hadoop fs -get $HDFS_RESULT_HOME/prelytix_extract/$day/$hour/extractUrl/* $local_result_path/
	gzip -d $local_result_path/*

	awk -F '\t' 'BEGIN{OFS="\t"}{print $1,$2,$3}' $local_result_path/* >> $local_result_path/url-list-$stime
}

function proc_result_log_extract(){
	echo "Transfer prelytix logs to s3 ..."
}

function proc_result_prelytix_extract(){
	proc_result_url_extract
	proc_result_log_extract
}

proc_result_prelytix_extract