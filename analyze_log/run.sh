#!/bin/bash
#date=`date +%Y%m%d -d yesterday`
date=`date +%Y%m%d`
#logType="bid imp win clk"
#logType="bid imp win clk"
logType="imp"
localLog="/yundisk/workspace/logProcess_local/log/"
localresult="/yundisk/workspace/logProcess_local/result/"
filePath="${localLog}${date}/"
proLogPath="/user/bilinhadoop/data_log/"
function getLog_HDFS()
{
	for i in $logType
	do
		logpath="${proLogPath}/${i}/${date}/${1}/*${i}*"
		outpath="${filePath}${1}/${i}.log"
		if [ ! -d "${filePath}${1}" ];then
			mkdir -p "${filePath}${1}"
			echo "copying to ${filePath}"
		fi
		echo "get file $logpath"
		`hadoop fs -getmerge ${logpath} ${outpath}`
	done
}

function getLog_LOCAL()
{
	if [ $1 ];then
		HOUR=$1
	else
		HOUR="2014110309"
	fi
	SERVER_IP1="172.18.0.24"
	SERVER_IP2="172.18.0.32"
	SERVER_IP3="172.18.0.75"
	#SERVER_IP4="172.18.0.39"
	#SERVER_IP5="172.18.0.51"
	#SERVER_IP6="172.18.1.8"
	SOURCE_LOG_PATH="/yundisk/log/hadoop"
	for i in $logType
	do
		#for j in {1..6}
		for j in {1..3}
		do
			var=SERVER_IP${j}
			scp bilin@${!var}:${SOURCE_LOG_PATH}/hadoop.${i}.tanx.bidder${j}.v6_0_2.${HOUR}.log ../${i}_${j}
		done
		cat ../${i}_* > ../${i} && rm ../${i}_*
	done
}

function getResult()
{
	#inputPath="${filePath}/$1/"
	inputPath="../"
	outputPath="/tmp/result.txt"
	resultPath="${localresult}/${date}/$1/"
	if [ ! -d $resultPath ];then
		mkdir -p $resultPath
	fi

	#python ./split.py ${inputPath}clk $outputPath clk && sort -k 1 $outputPath | awk -F '\t' \
	#'{if($1==before){value=value+$2}else{if(NR==1){before=$1;value=$2}else{print before"\t"value; before=$1;value=$2}}}END{print before"\t"value}'> ${resultPath}clk.txt && rm $outputPath && echo "clk log done" || exit $?
	#python ./split.py ${inputPath}imp $outputPath imp && sort -k 1 $outputPath | awk -F '\t' \
	#'{if($1==before){value=value+$2}else{if(NR==1){before=$1;value=$2}else{print before"\t"value; before=$1;value=$2}}}END{print before"\t"value}'> ${resultPath}imp.txt && rm $outputPath && echo "imp log done" || exit $?
	#python ./split.py ${inputPath}bid $outputPath bid && sort -k 1 $outputPath | awk -F '\t' \
	#'{if($1==before){value=value+$2;price=price+$3}else{if(NR==1){before=$1;value=$2;price=$3}else{print before"\t"value"\t"price; before=$1;value=$2;price=$3}}}END{print before"\t"value"\t"price}'> ${resultPath}bid.txt && rm $outputPath && echo "bid log done" || exit $?
	python ./split.py ${inputPath}imp $outputPath win && sort -k 1 $outputPath | awk -F '\t' \
	'{if($1==before){value=value+$2;price=price+$3;a[$4]++}else{if(NR==1){before=$1;value=$2;price=$3;a[$4]++}else{print before"\t"value"\t"price"\t"length(a); before=$1;value=$2;price=$3;delete a;a[$4]++}}}END{print before"\t"value"\t"price"\t"length(a)}'> ${resultPath}win.txt && rm $outputPath && echo "win log done" || exit $?
#sort -k 1 $outputPath | awk -F '\t' '{if($1==before){value=value+$2;price=price+$3}else{if(NR==1){before=$1;value=$2;price=$3}else{print before"\t"value"\t"price; before=$1;value=$2;price=$3}}}'> bid.txt && rm $outputPath &&     echo "bid log done" || echo "error" && exit $?
}

function sec_deal()
{
	#every 10 min; with return ( time	impressions	costs	clicks)
	awk -F '|' '{if($2==100189 && $1=="lineitem") print $0,"\t",$5}' ${localresult}${date}/${1}/win.txt | \
	awk -F '\t' '{a[$5]+=$2;b[$5]+=$3}END{for(i in a) print i,"\t",a[i],"\t",b[i]}' | sort > ../win-result
	
	awk -F '|' '{if($2==100189 && $1=="lineitem") print $0,"\t",$5}' ${localresult}${date}/${1}/clk.txt | \
	awk -F ' ' '{a[$3]+=$2}END{for(i in a) print i,"\t",a[i]}' | sort > ../clk-result
	
	join -a 1  ../win-result  ../clk-result > ../result.txt && rm ../win-result ../clk-result
}
#function run()
#{
#	#times=`date +%H -d last-hour`
#	cleanDate=`date +%Y%m%d -d '2 days ago'`
#	tm="08 09 10 11 12 13 14 15 16 17 18"
#	for k in $tm
#	do
#		getLog $k
#		getResult $k
#	done
#	if [ -d ${localLog}${cleanDate} ];then
#		rm -rf ${localLog}${cleanDate}
#	fi
#}
#getLog_LOCAL && getResult #&& sec_deal
getResult
#getResult && sec_deal
