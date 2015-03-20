#!/bin/bash
#
#
#

# import shell
LOCAL_JOB_HOME="$HOME/bilin_job"
source $LOCAL_JOB_HOME/common/aws_function.sh
source $LOCAL_JOB_HOME/logprocess/logprocess.sh
source $LOCAL_JOB_HOME/pixel_process/pixel_process.sh
source $LOCAL_JOB_HOME/prelytix_extract/prelytix_extract.sh

#run
ALL_STEP_ID=""
ALL_JOBS="pixel_process prelytix_extract"
# param 1 {stime}
function add_step2queue(){
	for job in $ALL_JOBS;do
		echo "START: $job $1" >> $LOCAL_LOG_PATH/${job}.log
		step_id=$(cmt_$job $1) >> $LOCAL_LOG_PATH/${job}.log
		result=$?
		if [ $result -eq 1 ];then
			echo "$step_id" >> $LOCAL_LOG_PATH/${job}.log
			echo "Warning : input logs do not exits" >> $LOCAL_LOG_PATH/${job}.log
			continue
		elif [ $result -ne 0 ];then
			send_email "ERROR: $stime commit job $job error"
			echo "$step_id" >> $LOCAL_LOG_PATH/${job}.log
			echo "ERROR: $stime commit job $job error" >> $LOCAL_LOG_PATH/${job}.log
			continue
		fi


		local step_job=${step_id}+${job}" "
		ALL_STEP_ID+=$step_job
		echo "Added $job step to emr $step_id"
	done
	echo "All job : $ALL_STEP_ID"
}

# param 1 {stime}
function wait_step_finish(){
	
	for step in $ALL_STEP_ID;do
		local step_name=$(echo $step | cut -d "+" -f 2)
		local step_id=$(echo $step | cut -d "+" -f 1)
		echo "wait $step_name $step_id finish ..."
		wait_for_complete $step_id >> $LOCAL_LOG_PATH/${step_name}.log  \
			|| (send_email "error in emr job : $step_name hadoop job" && continue)

		(proc_result_$step_name $1 >> $LOCAL_LOG_PATH/${step_name}.log && \
			echo "END: $step_name $1" >> $LOCAL_LOG_PATH/${step_name}.log || \
			send_email "error in emr job : $step_name process result") &
	done
}

function main(){
	local stime=$(date -d "-1 hours" +%Y%m%d%H)

	# wait until logs ready
	is_logs_ready $stime || (send_email "logs in s3 error" && return 1)

	# run logprocess
	echo "START: bilin emr job $stime"
	echo "START: $stime" >> $LOCAL_LOG_PATH/logprocess.log
	
	cmt_logprocess "$LOGS" $stime >> $LOCAL_LOG_PATH/logprocess.log   
	(proc_result_logprocess "$LOGS" $stime >> $LOCAL_LOG_PATH/logprocess.log && \
		echo "Job Done : $stime" >> $LOCAL_LOG_PATH/logprocess.log || \
		 send_email "error in emr job logprocess") & 
	
	echo "END: logprocess job"
	
	echo "START: $ALL_JOBS"
	add_step2queue $stime
	wait_step_finish $stime

	result=$(echo $?)
	echo "END: $result"
	if [ $result -ne 0 ];then
		send_email "error in emr log process"
	fi
	echo "END: $ALL_JOBS"
}
main
#=============================================================================

function run(){
	stime=$(date -d "-1 hours" +%Y%m%d%H)
	echo "$stime job start ..."
	add_step2queue $stime
	wait_step_finish $stime || echo "ERROR"
}
