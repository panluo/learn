#!/bin/bash
#
#


# import shell
LOCAL_JOB_HOME="$HOME/bilin_job"
source $LOCAL_JOB_HOME/common/aws_function.sh
source $LOCAL_JOB_HOME/logprocess/logprocess.sh
source $LOCAL_JOB_HOME/pixel_process/pixel_process.sh
source $LOCAL_JOB_HOME/prelytix_extract/prelytix_extract.sh

function run(){
	for day in 11;do
		hh="00 01 02 03 04 05 06 07 08 09"
		hhh=`seq 10 23`
		hours="$hh $hhh"
		for hour in $hours;do
			run_manual_prelytix_extract 201501$day$hour
		done
	done
}

run_manual_logprocess "$LOGS" 2015020906
