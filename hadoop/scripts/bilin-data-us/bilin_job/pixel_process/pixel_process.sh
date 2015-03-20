#!/bin/bash
#
#
#

# param 2 {input,output}
function cmt_pixel_process_step(){
	step_id=$(aws emr add-steps --cluster-id $CLUSTER_ID \
	--steps Type=CUSTOM_JAR,Name=pixel_process,ActionOnFailure=CONTINUE,Jar=$S3_JOB_HOME/logprocess/logProcess-pixel-3.0.0.jar,Args=com.bilin.main.PixelProcessor,$1,$2,pixel,$S3_JOB_HOME/logprocess/pixel-3.0.0.properties) || return 1
	step_id=$(echo $step_id | jq .StepIds[0] | sed 's/\"//g')
	echo "$step_id"
	echo 
}

# param 1 {stime:YYYYMMDDHH}
# return step_id
function cmt_pixel_process(){
	local day=${1:0:8}
	local hour=${1:8:2}

	local input=$S3PATH/data_log/pixel/$day/$hour/
	local output=$S3PATH/result/pixel/$day/$hour/
	local input_lines=$(aws s3 ls $input | wc -l)
	if [ $input_lines -eq 0 ];then
		echo "pixel logs are not exist"
		return 1
	fi
	local output_file=$(aws s3 ls $S3PATH/result/pixel/$day/$hour |wc -l)
	if [ $output_file -eq 1 ];then
		echo "pixel logs result of $1 already exist"
		return 2
	fi

	local step_id=""
	step_id=$(cmt_pixel_process_step $input $output) || ( echo "commit pixel_process step error" && return 3)
	echo "$step_id"
}
	
# param 1 {stime:YYYYMMDDHH}
function proc_result_pixel_process(){
	local day=${1:0:8}
	local hour=${1:8:2}
	local local_result_path="$LOCAL_RESULT_PATH/pixel_process/$1"
	test -e $local_result_path && rm -fr $local_result_path
	mkdir -p $local_result_path
	aws s3 sync $S3PATH/result/pixel/$day/$hour $local_result_path || return 1
	echo "Copy result to local successfully. Start store result to database"
	python ${LOCAL_JOB_HOME}/pixel_process/pixel_store2db.py $local_result_path || return 2
	rm -fr $local_result_path
	echo "Job pixel_process of $1 have completed."
}
