#!/bin/bash
# Program
#	Daily Report for Tim. Generate two file every day, one for creative and another for domain.
# Upload the two file to path s3://bilin-adops/reports/.
# History
#	2015-02-03	mataotao	1.0


MAIL_TO="taotao.ma@bilintechnology.com xinxin.shi@bilintechnology.com"

file_path=/yundisk/bilin-data/result/daily_report
function run(){
	day=$(date -d "-1 days" +%Y-%m-%d)
	echo "START: $day"
	python $HOME/bilin_task/daily_report/daily_report.py $day $file_path || return 1
	aws s3 cp $file_path/campaign_${day}.csv s3://bilin-adops/reports/ || return 2
	aws s3 cp $file_path/domain_${day}.csv s3://bilin-adops/reports/ || return 2
	rm -fr $file_path/campaign_${day}.csv $file_path/domain_${day}.csv
}
run
result=$?
echo "END: $result"
if [ $result -ne 0 ];then
	echo "Daily report result: $result" | mail -s 'Error in daily report' $MAIL_TO
fi
