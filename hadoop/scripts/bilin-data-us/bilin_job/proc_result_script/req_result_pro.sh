#!/bin/bash
# analyze request log everyday, count the frequency of every item
# @author : LuoPan
# @Date : 2014-09-23
# @run time : 3:20

date=$1
items="exchange_user_id bilin_user_id user_ip language size domain site_category user_category page_type user_gender page_vertical ad_slot_visibility creative_type referer os browser view_type view_screen geo flash publisher_id device url"
EXCS="bidswitch"
SSPS="pulsepoint adconductor cox rubicon pubmatic admeta"


output="/user/hadoop/result/log_analysis/req/$date/"
function sorts(){
	resultPath="/mnt/bilin/tmp/request/${date}"
	if [ ! -d $resultPath ];then
		mkdir -p $resultPath
	fi
	test -e $resultPath && rm -fr $resultPath
	mkdir -p $resultPath
	for item in $items
	do
		for exchange in $EXCS
		do
			name="${exchange}.${item}.${date}"
			filename="${output}${name}"
			
			hadoop fs -test -e ${filename}.cnt-r-00000 || hadoop fs -test -e ${filename}.frq-r-00000
			if [ $? -eq 0 ];then
				item_frq="${filename}.frq*"
				item_cnt="${filename}.cnt*"

				file_frq="${resultPath}/${name}.frq"
				file_cnt="${resultPath}/${name}"
				
				`hadoop fs -getmerge ${item_cnt} ${file_cnt}`
				`hadoop fs -getmerge ${item_frq} ${file_frq}`
				if [ $exchange = "bidswitch" ];then
					(awk -F '\t' '{val=$1".""'$item'""."'$date'".frq";print $2"\t"$3 >> "'$resultPath'/"val;}' ${file_frq} &&
					for ssp in $SSPS
					do
						file_name="${resultPath}/${ssp}.${item}.${date}.frq"
						if [ -e $file_name ];then
							sort -k 2 -t "	" -n -r ${file_name} -o ${file_name} && lines=`wc -l ${file_name} | awk '{print $1}'` &&  echo "${ssp}	KINDS	$lines" >> $file_cnt || echo "${file_name} sorted fiald"
						fi
					done
					rm $file_frq
					)&
				else
					(sort -k 2 -t "	" -n -r ${file_frq} -o ${file_frq} || \
						echo "${file_frq} sort filed" && \
						lines=`wc -l $file_frq | awk '{print $1}'`
						echo "KINDS	$lines" >> $file_cnt )&
				fi
			fi
		done
	done

	cntFile="$resultPath/*.${date}"
	for k in `ls $cntFile`
	do
		name="$k.cnt"
		sort -k 3 -t "	" -n -r $k -o $name && rm $k || echo "$name sort failed"
	done

	rm ${resultPath}/.*.crc
}
sorts
echo "Start sync result to s3 ..."
for i in 1 2 3;do
	aws s3 sync --quiet $resultPath s3://bilin-data/hadoop/result/log_analysis/req/$date
done

echo "Result has transfered to s3 successfully"
