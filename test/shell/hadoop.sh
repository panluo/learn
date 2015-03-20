#!/bin/bash

#Hadoop jar
date=`date +%Y%m%d -d yesterday`
#echo $date
if [ $1 ]; then
    input=$1
else 
    input="/user/luo/log" 
fi
echo "input file path: " $input

if [ $2 ]; then
    output=$2
   # echo $output
else
    output="/user/luo/output/${date}"
fi
echo "output file path: "  $output

function sorts(){
	currenPath=`pwd`
	echo "current path: $currentPath"
	filePath=$currentPath/$date
	echo "file path: $filePath"
	if [ -e $filePath ];then
		rm -rf $date
	fi
	`hadoop fs -get $output`

	for i in `ls $filePath`
	do
		filename=$filePath/$i
		if [ -f $filename ]; then
			sort -k 2 -n -r $filename > tmp && cat tmp > $filename
			echo "$i done"
		fi
		#echo "hello"
	done
}

`hadoop jar /home/bilinhadoop/Frequencies-1.0.0.jar bilin_hadoop.Frequency $input $output` && sorts
#`hadoop fs -get /user/luo/output/${date}`
#sorts
