#!/bin/bash
#
#	Request log aggregation
#
#
#



SCRIPT_PATH="$HOME/bilin_job"

#
source $SCRIPT_PATH/common/aws_function.sh


# job env
CLUSTER_ID=""
REQ_STEP_ID=""
REQ_CONF_FILE="$S3PATH/job/request_process/FRE.properties"


#===============================================================================
function create_req_cluster(){
	CLUSTER_ID=$(aws emr create-cluster --name req_cluster --ami-version 3.3.1 \
	--instance-groups Name=Masternode,InstanceGroupType=MASTER,InstanceType=c3.xlarge,InstanceCount=1,BidPrice=0.2 \
	Name=Corenode,InstanceGroupType=CORE,InstanceType=c3.xlarge,InstanceCount=2,BidPrice=0.2 \
	Name=Tasknode,InstanceGroupType=TASK,InstanceType=c3.xlarge,InstanceCount=2,BidPrice=0.2 \
	--no-auto-terminate \
	--ec2-attributes KeyName=bilin2014_east \
	--use-default-roles \
	--log-uri s3://bilin-data/hadoop/emrlog \
	--applications Name=Hive Name=Ganglia \
	--enable-debugging \
	--bootstrap-actions Path=s3://elasticmapreduce/bootstrap-actions/configure-hadoop,Name=Hadoop_configure,Args=["-m","mapreduce.job.reduce.slowstart.completedmaps=0.80","-m","mapreduce.map.output.compress=true"] \
	Path=s3://bilin-data/hadoop/bilin_emr/bootstrap/bilin_emr_bs.sh,Name=Bilin_job_sync) || return 1
	CLUSTER_ID=$(echo $CLUSTER_ID | jq .ClusterId | sed 's/\"//g')  || return 2
	echo "Req cluster ID: $CLUSTER_ID"
#	--bootstrap-action Path=s3://elasticmapreduce/bootstrap-actions/configure-hadoop,Args=["-y","yarn.scheduler.maximum-allocation-mb=2048","-y","yarn.scheduler.maximum-allocation-vcores=4"]) || return 1
	
}

# Terminate the running cluster
# no param
function term_cluster(){
	aws emr terminate-clusters --cluster-ids $CLUSTER_ID
}
#=============================================================================
# Add request process step to cluster
# params 4 {input,output,configure file path,date in output filename default: sys time}
# return 1 if commit step failed
function cmt_request_process_step(){
	REQ_STEP_ID=$(aws emr add-steps --cluster-id $CLUSTER_ID \
		--steps Type=CUSTOM_JAR,Name=req_process,ActionOnFailure=CONTINUE,Jar=s3://bilin-data/hadoop/job/request_process/Frequencies-2.0.0.jar,Args=bilin_hadoop.Frequency2,$1,$2,$3,$4 \
			) || return 1
	REQ_STEP_ID=$(echo $REQ_STEP_ID | jq .StepIds[0] | sed 's/\"//g')
	echo "Step id : $REQ_STEP_ID"
}

# add request process step to cluster for prelytix
# Commit job to cluster
# params 1 {day time:YYYYMMDD}
function cmt_request_process(){

	local input=$S3PATH/data_log/req/$1/
	#output file is in hdfs
	local output=/user/hadoop/result/log_analysis/req/$1/
	
	local input_lines=$(aws s3 ls $input | wc -l)
	if [ $input_lines -eq 0 ];then
		echo "request logs of $1 are not exist"
		return 0
	fi

	cmt_request_process_step $input $output $REQ_CONF_FILE $1 || return 2
	echo "process request log"
}

# Copy ETL result from s3 to local fs, then store to mysql db.
# param 1 {sday:YYYYMMDD}
function s3_to_db(){
	sday=$1
	echo "Start transfer log to cms server"
	local_result_path="$LOCAL_RESULT_PATH/log_analysis/req/$sday"
	test -e $local_result_path && rm -fr $local_result_path
	mkdir -p $local_result_path
	aws s3 sync --quiet $S3_RESLUT_HOME/log_analysis/req/$sday $local_result_path || return 5
	
	echo "Has transfered result to cms server. Start store2db..."
	python $SCRIPT_PATH/request_process/req_store2db.py $local_result_path || return 5
	echo "JOB SUCCESS"
	rm -fr $local_result_path

}


# Run log processing job
# param 1 {sday:YYYYMMDD}
function run_req_job(){
	
	# local sday=$(date -d "-1 days" +%Y%m%d)
	sday=$1
	echo "$sday req aggreagete start ..."
	echo "create cluster ..."
	create_req_cluster || return 1

	# if req logs of last day are transfered complete from bidder server to s3 finished
	# local count=$(aws s3 ls $S3PATH/data_log/status/ | grep "$sday" | wc -l)
	# if [ $count -ne 0 ];then
	# 	echo "request log transfer from bidder server to s3 error"
	# 	return 1
	# fi

	cmt_request_process $sday || return 2

	wait_for_complete $REQ_STEP_ID || return 3

	echo "Job done, start integrate the result"
	aws emr ssh --cluster-id $CLUSTER_ID  --key-pair-file $KEY_PAIR_FILE  \
		--command "sh $EMR_JOB_HOME/request_process/req_result_pro.sh $sday" || return 4
	
	term_cluster
	
	echo "Start transfer log to cms server"
	local_result_path="$LOCAL_RESULT_PATH/log_analysis/req/$sday"
	test -e $local_result_path && rm -fr $local_result_path
	mkdir -p $local_result_path
	aws s3 sync --quiet $S3_RESLUT_HOME/log_analysis/req/$sday $local_result_path || return 5
	
	echo "Has transfered result to cms server. Start store2db..."
	python $SCRIPT_PATH/request_process/req_store2db.py $local_result_path || return 5
	echo "JOB SUCCESS"
	rm -fr $local_result_path
}

sday=$(date -d "-1 days" +%Y%m%d)
#sday=20150202
run_req_job $sday || ( echo ERROR && send_email "ERROR:process request log" )
echo "Result: $?"
term_cluster

