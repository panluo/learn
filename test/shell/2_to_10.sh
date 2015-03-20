#!/bin/bash
#vim:set sw=4 ts=4 et:
help()
{
	cat << HELP
	b2d -- convert binary to decimal
	USAGE: b2b
	OPTIONS: -h help text
HELP
	exit 0
}

error()
{
	echo "$1"
	exit 1
}

lastchar()
{
	if [ -z "$1" ]; then
		#empty string
		rval=""
		return
	fi
	numofchar=`echo -n "$1"| sed 's/ //g' | wc -c `
	rval=`echo -n "$1" | cut -b $numofchar`
}

chop()
{
	if [ -z "$1" ];then
		rval=""
		return
	fi
	nomofchar=`echo -n "$1" | wc -c | sed 's/ //g' `
	if [ "$numofchar" = "1" ];then
		rval=""
		return
	fi
	numofcharminus1=`expr $numofchar "-" 1`
	# now cut all but the last char:
	rval=`echo -n "$1" | cut -b -$numofcharminus1` 
}

while [ -n "$1" ]; do
	case $1 in
		-h) help;shift 1;; # function help is called
		--) shift;break;; # end of options
		-*) error "error: no such option $1. -h for help";;
		*) break;;
	esac
done

# The main program
sum=0
weight=1

# one arg must be given:
[ -z "$1" ] && help
binnum="$1"
binnumorig="$1"

while [ -n "$binnum" ]; do
	lastchar "$binnum"
	if [ "$rval" = "1" ]; then
		sum=`expr "$weight" "+" "$sum"`
	fi
	# remove the last position in $binnum
	chop "$binnum"
	binnum="$rval"
	weight=`expr "$weight" "*" 2`
done

echo "binary $binnumorig is decimal $sum"
