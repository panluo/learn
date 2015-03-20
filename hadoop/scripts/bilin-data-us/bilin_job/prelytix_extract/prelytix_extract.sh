#!/bin/bash
#
#	Extract url from request log 
#


EXTRACT_FACTOR=100
PE_SET_ID="14"

function cmt_prelytix_extract_step(){
	local step_id=$(aws emr add-steps --cluster-id $CLUSTER_ID \
	--steps Type=CUSTOM_JAR,Name=prelytix_extract,ActionOnFailure=CONTINUE,Jar=$S3_JOB_HOME/prelytix_extract/RequestExtractUrl-1.0.4.jar,Args=com.bilin.main.Processor,$1,$2,$3,$4,$5,$6,2 \
		) || return 1
	step_id=$(echo $step_id | jq .StepIds[0] | sed 's/\"//g')
	echo "$step_id"
}

# param 1 {stime:YYYYMMDDHH}
function cmt_prelytix_extract(){
	local day=${1:0:8}
	local hour=${1:8:2}

	local req_input=$S3PATH/data_log/req/$day/*$1*
	local win_input=$S3PATH/data_log/win/$day/$hour/
	local output=/user/hadoop/result/prelytix_extract/$day/$hour/
	local conf_file_path=$S3_JOB_HOME/prelytix_extract/extracturl-1.0.5.properties
	
	local input_lines_req=$(aws s3 ls $S3PATH/data_log/req/$day/ | grep "$1" | wc -l)
	if [ $input_lines_req -eq 0 ];then
		echo "req logs are not exist"
		return 1
	fi

	local input_lines=$(aws s3 ls $win_input | wc -l)
	if [ $input_lines -eq 0 ];then
		win_input=0
	fi

	cmt_prelytix_extract_step $req_input $win_input $output req $conf_file_path $EXTRACT_FACTOR || return 1
}
	
# param 1 {stime}
function proc_result_prelytix_extract(){
	echo "process result of prelytix_extract"
	local stime=$1
	local day=${1:0:8}
	local hour=${1:8:2}
#	local local_result_path="$LOCAL_RESULT_PATH/prelytix_extract/$stime"
#	local emr_result_path="$EMR_RESULT_HOME/prelytix_extract/$stime"
#	
#	test -e $local_result_path && rm -fr $local_result_path
#	mkdir -p $local_result_path
#

	aws emr ssh --cluster-id $CLUSTER_ID  --key-pair-file $KEY_PAIR_FILE  \
		--command "sh $EMR_JOB_HOME/prelytix_extract/proc_result_prelytix_extract.sh $stime" || return 1
#	echo "scp prelytix_extract url result to cms server"
#	
#	for set_id in $PE_SET_ID;do
#		scp hadoop@$MASTER_IP:$emr_result_path/url-list-$set_id $local_result_path/  || return 2
#		echo "push $stime result to redis. set id $set_id"
#		curl -T $local_result_path/url-list-$set_id "54.164.24.124/upload_list?type=u&op=add" || return 3
#		curl -T $local_result_path/url-list-$set_id "54.67.12.41/upload_list?type=u&op=add" || return 4
#	done
#	echo "rm result in emr local"
#	ssh hadoop@$MASTER_IP rm -fr $emr_result_path  \
#	|| echo "warn : prelytix_extract result in emr local fs does not delete successfully"
#	#aws s3 sync $LOCAL_RESULT_PATH/prelytix_extract s3://bilin-data/hadoop/result/prelytix_extract
#	rm $local_result_path -fr

}

# param 1 {stime}
function run_manual_prelytix_extract(){        
    local step_id=`cmt_prelytix_extract $1`
	wait_for_complete $step_id
	proc_result_prelytix_extract $1
}
