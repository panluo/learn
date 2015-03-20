#!/bin/bash

HOSTNAME="localhost"
PORT="3306"
USERNAME="root"
PASSWORD=""

DBNAME="report_system"
TABLENAME="rs"
SHOWBD="show databases"
USEREP="use report_system"
yesterday=`date +%Y-%m-%d -d yesterday`
filePath="/tmp/result/"
outfile="/tmp/result.txt"
if [ ! -d $filePath ];then
	mkdir $filePath
fi

#if [ -f $outfile ];then
#	 expect -c "su -
#	 set timeout 10
#	 expect \"*\"
#	 set timeout 10
#	 send \"wkzwhu2004\"
#	 set timeout 10
#	 send \"rm $outfile\r\"
#	 send \"exit\r\"
#	 expect eof
#	 exit"
# fi
#mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} -e "use ${DBNAME}"
#echo $yesterday
#mysql -u ${USERNAME} -e "$USEREP;select dimension_value from rs where dimension_type='creative' and lineitem_id='100072' and time>='2014-09-02 00:00:00' and time<='2014-09-02 23:00:00' into outfile '/tmp/result1.txt';"
#mysql -u ${USERNAME} -e "${USEREP}; select dimension_value,lineitem_id,impressions,clicks,costs from rs where lineitem_id='100072' and dimension_type='domain' and time>='2014-09-02 00:00:00' and time<='2014-09-02 23:00:00' into outfile '$outfile';"
cat ${outfile} | awk -F '\t' '{if(before==$1 && item==$2){total_imp=total_imp+$3;total_clicks=total_clicks+$4;total_costs=total_costs+$5}else{ print before,"\t",item,"\t",total_imp,"\t",total_clicks,"\t",total_costs; total_imp=$3; total_clicks=$4; total_costs=$5; before=$1; item=$2}}END{print before,"\t",item,"\t",total_imp,"\t",total_clicks,"\t",total_costs;}' | awk '
function cha(a,b){
        return b>0?a/b:0;
	}
	{print $1,"\t",$2,"\t",$3,"\t",$4,"\t",cha($4,$3)*100"%","\t",cha($5,$4)/100000,"\t",cha($5,$3)/100,"\t",$5/100000}' | join -j 1 -a 1 -t "\t" -e "empty" --nocheck-order - /home/luopan/test.txt > result.txt
