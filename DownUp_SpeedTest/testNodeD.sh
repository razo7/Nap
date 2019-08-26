#!/bin/bash
# testNodeD.sh
# Author: Or Raz
#
slaveFile="slaveFile-file"
#read from file the other slaves
input="/usr/local/hadoop/etc/hadoop/slaves"
#while IFS= read -r var
#do
#        let slave_num+=1
#        ssh_PCs+=($var)
#done < "$slaveFile"
ssh_PCs=$(head -n 1 $slaveFile)
slave_num=$(tail -n 1 $slaveFile)
test_file="scp-test-file"
AVG_downlink_PCs=0
rounds=4
for ssh_PC in ${ssh_PCs[@]} ; do
        # download test
#        echo "Testing download from $ssh_PC..."
	for ((idx=0; idx<$rounds; idx++)); do
		temp_speed=`scp -v $ssh_PC:$test_file $test_file 2>&1 | \
                  grep "Bytes per second" | \
                  sed "s/^[^0-9]*\([0-9.]*\)[^0-9]*\([0-9.]*\).*$/\2/g"`
		temp_speed=`echo "($temp_speed*8*0.000001)/1" | bc`
		let down_speed+=$temp_speed
	done
	down_speed=`echo "(($down_speed/$rounds)+0.5)/1" | bc`
        let AVG_downlink_PCs+=$down_speed
	Downlink_PCs+=($down_speed)
#        echo "AVG : $AVG_downlink_PCs"
done
AVG_downlink_PCs=`echo "(($AVG_downlink_PCs/$slave_num)+0.5)/1" | bc`
# clean up
#echo "Removing test file locally..."
#`rm $test_file`
# print result
#echo "Average Downlink speed[Mb/s] is $AVG_downlink_PCs"
echo "${Downlink_PCs[*]}" > resFile-file
echo "$AVG_downlink_PCs" >> resFile-file
