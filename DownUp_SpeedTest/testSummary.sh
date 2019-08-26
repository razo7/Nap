#!/bin/bash
# testSummary.sh
# Author: Or Raz
#
# Topology - {East- (1,2), West- 3, London- (4,5)}
if [[ $# -eq 1 ]] ; then
	echo 'some message'
	`bash testClusterD.sh slave1 slave2 slave3 slave4 slave5`
fi
avg_file="downLink.txt"
#while IFS=" " read -r var1 var2 var3 var4 var5; do
#        echo $var2
#done < <(tail -n +3 $avg_file)
s1=($(sed '3q;d' $avg_file))
s2=($(sed '4q;d' $avg_file))
s3=($(sed '5q;d' $avg_file))
s4=($(sed '6q;d' $avg_file))
s5=($(sed '7q;d' $avg_file))
east=`echo "(((${s1[1]}+${s2[1]})/2)+0.5)/1" | bc`
eastToWest=`echo "(((${s1[2]}+${s2[2]})/2)+0.5)/1" | bc`
eastToLondon1=`echo "(((${s1[3]}+${s2[3]})/2)+0.5)/1" | bc`
eastToLondon2=`echo "(((${s1[4]}+${s2[4]})/2)+0.5)/1" | bc`
eastToLondon=`echo "((($eastToLondon1+$eastToLondon2)/2)+0.5)/1" | bc`
westToEast=`echo "(((${s3[1]}+${s3[2]})/2)+0.5)/1" | bc`
westToLondon=`echo "(((${s3[3]}+${s3[4]})/2)+0.5)/1" | bc`
london=`echo "(((${s4[4]}+${s5[4]})/2)+0.5)/1" | bc`
londonToEast1=`echo "(((${s4[1]}+${s5[1]})/2)+0.5)/1" | bc`
londonToEast2=`echo "(((${s4[2]}+${s5[2]})/2)+0.5)/1" | bc`
londonToEast=`echo "((($londonToEast1+$londonToEast2)/2)+0.5)/1" | bc`
londonToWest=`echo "(((${s4[3]}+${s5[3]})/2)+0.5)/1" | bc`
printf "East:\nlocal= $east,from West= $eastToWest, from London1= $eastToLondon1, from London2= $eastToLondon2, from London= $eastToLondon\n"
printf "West:\nfrom East= $westToEast, from London= $westToLondon\n"
printf "London:\nlocal= $london,from West= $londonToWest, from East1= $londonToEast1, from East2= $londonToEast2, from East= $londonToEast\n"
