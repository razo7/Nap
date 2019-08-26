#!/bin/bash
# testClusterD.sh
# Author: Or Raz
calc(){ awk "BEGIN { print "$*" }"; }
splitSlaveArrayByIndex (){
	if [ "$i" = 1 ]; then
		        updateList=("${ssh_PCs[@]:i}")
        else
	               updateList=("${ssh_PCs[@]:0:i-1} ${ssh_PCs[@]:i}")
	fi
	echo "${updateList[@]}" > $slaveFile
	echo "$num_slaves" >> $slaveFile
}
normalizeArray (){
	sortedColNums=( $( printf "%s\n" "${downlink_PCs[@]}" | sort -n ) )
	min=${sortedColNums[0]}
	normalizedMin=$(calc $min/10)
	normalizedArray=()
	for dLink in ${downlink_PCs[@]} ; do # upload test & slaveFile
#		normalizedArray+=("$((dLink/normalizedMin))")
		normalizedArray+=($(calc $dLink/$normalizedMin))
	done
}
test_size="50" # Test file size in MBs
ssh_PCs=()
downVec_PCs=()
downlink_PCs=()
i=0
test_file="scp-test-file"
slaveFile="slaveFile-file"
AVGFile="dd2.sh"
resFile="resFile-file"
num_slaves=$#
((num_slaves=num_slaves-1))
for var in "$@"; do
	ssh_PCs+=($var)
done
# generate a test_size file of all zeros
echo "Generating $test_size MB test file..."
`dd if=/dev/zero of=$test_file bs=$(echo "$test_size*1024*1024" | bc) \
  count=1 &> /dev/null`
for ssh_PC in ${ssh_PCs[@]} ; do # upload test & slaveFile
	echo "Upload test file and Dlink-Test to $ssh_PC..."
	`scp $test_file $ssh_PC:$test_file`
	`scp $AVGFile $ssh_PC:$AVGFile`
	downVec_ISP_PCs+=(`ssh $ssh_PC ./Nap/DownUp_SpeedTest/speedtest-cli 2>&1 | \
                  grep "Download:" | \
                  sed "s/^[^0-9]*\([0-9.]*\)[^0-9]*\([0-9.]*\).*$/\1/g"`)
done
printf "Average Downlink speed is in Mb/s\nDownlinkVec from ISP: (${downVec_ISP_PCs[*]})\n" | tee downLink.txt
for ssh_PC in ${ssh_PCs[@]} ; do
	let i+=1
	splitSlaveArrayByIndex
	`scp $slaveFile $ssh_PC:$slaveFile`
	`ssh $ssh_PC bash $AVGFile` #remote request to download the file from the cluster
	`scp $ssh_PC:$resFile $resFile` #get the average Dlink
	downVec_PCs1=$(head -n 1 $resFile)
	avgLink=$(tail -n 1 $resFile)
	echo "$ssh_PC: ($downVec_PCs1), avgLink: $avgLink" | tee -a downLink.txt
#	downVec_PCs+=($downVec_PCs1)
	downlink_PCs+=($avgLink)
done
for ssh_PC in ${ssh_PCs[@]} ; do # clean up
        echo "Removing test file on $ssh_PC..."
	`ssh $ssh_PC "rm $test_file $slaveFile $resFile"`
done
echo "Removing test file locally..."
normalizeArray
echo "Normailzed DownlinkVec: (${normalizedArray[*]})" | tee -a downLink.txt
#for ((idx=0; idx<${#downVec_PCs[@]}; idx+=$num_slaves)); do
#	echo "${ssh_PCs[idx/num_slaves]}: ${downVec_PCs[@]:idx:num_slaves}" >> downLink.txt
#done
