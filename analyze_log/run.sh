#!/bin/bash

outputPath="/tmp/logProcess_result.txt"
inputPath="/yundisk/luopan/"
python ./split.py ${inputPath}clk.log $outputPath clk && sort -k 1 $outputPath | awk -F '\t' '{if($1==before){value=value+$2}else{if(NR==1){before=$1;value=$2}else{print before"\t"value; before=$1;value=$2}}}END{print before"\t"value}'> clk.txt && rm $outputPath && echo "clk log done" || exit $?
python ./split.py ${inputPath}imp.log $outputPath imp && sort -k 1 $outputPath | awk -F '\t' '{if($1==before){value=value+$2}else{if(NR==1){before=$1;value=$2}else{print before"\t"value; before=$1;value=$2}}}END{print before"\t"value}'> imp.txt && rm $outputPath && echo "imp log done" || exit $?
python ./split.py ${inputPath}bid.log $outputPath bid && echo "split done" && sort -k 1 $outputPath | awk -F '\t' '{if($1==before){value=value+$2;price=price+$3}else{if(NR==1){before=$1;value=$2;price=$3}else{print before"\t"value"\t"price; before=$1;value=$2;price=$3}}}END{print before"\t"value"\t"price}'> bid.txt && rm $outputPath && echo "bid log done" || echo "error" && exit $?
python ./split.py ${inputPath}win.log $outputPath win && sort -k 1 $outputPath | awk -F '\t' '{if($1==before){value=value+$2;price=price+$3;a[$4]++}else{if(NR==1){before=$1;value=$2;price=$3;a[$4]++}else{print before"\t"value"\t"price"\t"length(a); before=$1;value=$2;price=$3;delete a;a[$4]++}}}END{print before"\t"value"\t"price"\t"length(a)}'> win.txt && rm $outputPath && echo "win log done" || exit $?
#sort -k 1 $outputPath | awk -F '\t' '{if($1==before){value=value+$2;price=price+$3}else{if(NR==1){before=$1;value=$2;price=$3}else{print before"\t"value"\t"price; before=$1;value=$2;price=$3}}}'> bid.txt && rm $outputPath &&     echo "bid log done" || echo "error" && exit $?
