#!/bin/bash
# dd2.sh
# Author: Or Raz
#

ssh_PCs=()
slave_num=0
slaveFile="slaveFile-file"
#read from file the other slaves
input="/usr/local/hadoop/etc/hadoop/slaves"
while IFS= read -r var
do
        let slave_num+=1
        ssh_PCs+=($var)
done < "$slaveFile"

test_file="scp-test-file"
AVG_downlink_PCs=0
for ssh_PC in ${ssh_PCs[@]} ; do
        # download test
#        echo "Testing download from $ssh_PC..."
        down_speed=`scp -v $ssh_PC:$test_file $test_file 2>&1 | \
          grep "Bytes per second" | \
          sed "s/^[^0-9]*\([0-9.]*\)[^0-9]*\([0-9.]*\).*$/\2/g"`
        down_speed=`echo "(($down_speed*0.0009765625*100.0+0.5)/1*0.01 +0.5)/1" | bc`
        let AVG_downlink_PCs+=$down_speed
#        echo "AVG : $AVG_downlink_PCs"
done
AVG_downlink_PCs=`echo "(($AVG_downlink_PCs/$slave_num)+0.5)/1" | bc`
# clean up
#echo "Removing test file locally..."
#`rm $test_file`
# print result
#echo "Average Downlink speed[kB/s] is $AVG_downlink_PCs"
echo "$AVG_downlink_PCs" > resFile-file
