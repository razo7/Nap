#!/bin/bash
# dd1.sh
# Author: Or Raz
splitSlaveArrayByIndex (){
	if [ "$i" = 1 ]; then
	        updateList=("${ssh_PCs[@]:i}")
        else
		updateList=("${ssh_PCs[@]:0:i-1} ${ssh_PCs[@]:i}")
	fi
	echo "${updateList[@]}" > $slaveFile
}
normalizeArray (){
	sortedColNums=( $( printf "%s\n" "${downlink_PCs[@]}" | sort -n ) )
	min=${sortedColNums[0]}
	normalizedArray=()
	for dLink in ${downlink_PCs[@]} ; do # upload test & slaveFile
		normalizedArray+=("$((dLink/min))")
	done
	echo "${normalizedArray[@]}"
}
test_size="100" # Optional: user specified test file size in kBs
ssh_PCs=()
uplink_PCs=()
downlink_PCs=()
i=0
test_file="scp-test-file"
slaveFile="slaveFile-file"
AVGFile="dd2.sh"
resFile="resFile-file"
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
done
for ssh_PC in ${ssh_PCs[@]} ; do
	let i+=1
	splitSlaveArrayByIndex
	`scp $slaveFile $ssh_PC:$slaveFile`
	`ssh $ssh_PC bash $AVGFile` #remote request to download the file from the cluster
	`scp $ssh_PC:$resFile $resFile` #get the average Dlink
	avg_Dlink=$(head -n 1 $resFile)
	downlink_PCs+=($avg_Dlink)
done
for ssh_PC in ${ssh_PCs[@]} ; do # clean up
        echo "Removing test file on $ssh_PC..."
	`ssh $ssh_PC "rm $test_file $slaveFile $resFile"`
done
#echo "Removing test file locally..."
printf "\nDownlink speed[kB/s]: (${downlink_PCs[*]})\n"
normalizeArray
echo "${normalizedArray[*]}" > downLink.txt
