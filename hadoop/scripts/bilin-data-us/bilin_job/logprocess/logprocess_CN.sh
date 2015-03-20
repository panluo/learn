#!/bin/bash
# Program
#	Build amazon emr cluster, then commit log process job to the cluster.
# History
#	2014-10-15	mataotao	v1.0.0
#	2014-10-29	mataotao	v1.0.1	Add print log and send email


# job parameters
LOGS="bid win imp clk cvs"
CONFPATH=$S3_HOME/job/logprocess/bilin_3.0.2.properties
IP_TO_GEO_PATH=$S3_HOME/job/logprocess/china.csv
#HOUR_INTERVAL=1  # how long to run the mapreduce (hours)


# Local env
# Result Path
LP_RESULT_PATH="$LOCAL_RESULT_PATH/logprocess"
PEM_FILE_PATH="$HOME/.bilin/bilin2014_east.pem"
PROFILE=bilin_cn


#==============================================================================

# Add step to cluster
# params 5 {input,output,logtype,configure file path,ip2geo file path}
# return 1 if commit step failed
# return step id
function cmt_logprocess_step(){
	local step_id=$(aws -profile $PROFILE emr add-steps --cluster-id $CLUSTER_ID \
	--steps Type=CUSTOM_JAR,Name=logprocess_$3,ActionOnFailure=CONTINUE,Jar=$S3_JOB_HOME/logprocess/logProcess-3.0.2.jar,Args=com.bilin.main.Processor,$1,$2,$3,$4,$5) || return 1
	step_id=$(echo $step_id | jq .StepIds[0] | sed 's/\"//g')
	echo "$step_id"
}


# Commit job to cluster
# params 2 {logs,stime}
function cmt_logprocess(){
	local day=${2:0:8}
	local hour=${2:8:2}
	for i in $1;
	do
		input=$S3PATH/data_log/$i/$day/$hour/
		output=$S3PATH/result/log_analysis/$i/$day/$hour/
		local input_lines=$(aws --profile $PROFILE s3 ls $input | wc -l)
		if [ $input_lines -eq 0 ];then
			echo "$i logs are not exist"
			continue
		fi
		local output_file=$(aws --profile $PROFILE s3 ls $S3PATH/result/log_analysis/$i/$day/$hour |wc -l)
		if [ $output_file -eq 1 ];then
			echo "$i logs result of $2 is exist"
			continue
		fi

		local step_id=$(cmt_logprocess_step $input $output $i $CONFPATH $IP_TO_GEO_PATH ) || return 1
		echo "process $i log"
		wait_for_complete $step_id || return 2
	done
}


# process result of log processing
# param 2 {logs,stime}
function proc_result_logprocess(){
	ssh -i $PEM_FILE_PATH ec2-user@ec2-54-85-3-210.compute-1.amazonaws.com sh /home/ec2-user/mataotao/bilin-data/logprocess/logprocess_CN.sh $2	|| return 1
}

# Store result to database
# param 2 {logtypes,time=YYYYMMDDHH}
# return 1 if sync from s3 error
# return 2 if store to database error
function store2db(){
	local day=${2:0:8}
	local hour=${2:8:2}
	local result_path=$LP_RESULT_PATH/$2
	test -e $result_path && rm -fr $result_path
	mkdir -p $result_path/last
	touch $result_path/last/success
	for log in $1;do
		mkdir -p $result_path/$log/$day/$hour
		
		local result_file=$(aws --profile $PROFILE s3 ls $S3PATH/result/log_analysis/$log/$day/$hour | wc -l)
		if [ $result_file -eq 0 ];then
			echo "$log result is not exists"
			continue
		fi
		
		aws --profile $PROFILE s3 sync --quiet $S3PATH/result/log_analysis/$log/$day/$hour $result_path/$log/$day/$hour || return 1
		echo "Sync $log to local fs"
		
		cat $result_path/$log/$day/$hour/* >> $result_path/last/$log-$2
		echo "$log-$2" >> $result_path/last/success
	done
	echo "Copy result to local successfully. Start store result to database"

	hour_sec=$(date -d "$day $hour" +%s)
	hour_sec=$(( $hour_sec/3600 ))

	python $LOCAL_JOB_HOME/logprocess/store2db_v3.py $hour_sec $result_path || return 2

	echo "store to rs_data"
	python $LOCAL_JOB_HOME/logprocess/store2db_v4.py $hour_sec $result_path || return 2
	echo "Store result to database complete."
	rm -fr $result_path
}




#===============================================================================

# param 1 {stime}
function sync_camp_table(){
	local jhour=$(echo $1 | sed -r 's/^(.{4})(.{2})(.{2})(.{2})$/\1-\2-\3 \4/g')
		
	for i in $SSP
	do
		mysql -u root -D report_system -e "replace into campaign_lineitem(campaign_id,lineitem_id) select distinct campaign_id,lineitem_id from $i where time='$jhour'"
		if [ $? -ne 0 ];then
			echo "Error occur when sync table campaign_lineitem"
			return 1
		fi
	done
}


#=============================================================================
# Run log processing job with given time
# param 2 {logs,stime:YYYYMMDDHH}
function run_manual_logprocess(){
#	cmt_logprocess "$1" $2	|| return 1
	store2db "$1" $2 || return 2
}
