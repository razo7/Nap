#!/bin/bash
# downlinkSpeed-test.sh
# Author: Or Raz
# Source: http://www.alecjacobson.com/weblog/?p=635
# Test ssh connection speed by uploading and then downloading a 10000kB test
# file (optionally user-specified size)
#
# Usage:
#   ./scp-speed-test.sh user@hostname [test file size in kBs]
#
test_file=".scp-test-file"
ssh_PCs=()
uplink_PCs=()
downlink_PCs=()
for var in "$@"; do
	ssh_PCs+=($var)
	echo "$var"
done


# Optional: user specified test file size in kBs
if test -z "$4"
then
  # default size is 10kB ~ 10mB
  test_size="10000"
else
  test_size=$4
fi


# generate a 10000kB file of all zeros
echo "Generating $test_size kB test file..."
`dd if=/dev/zero of=$test_file bs=$(echo "$test_size*1024" | bc) \
  count=1 &> /dev/null`
for ssh_PC in ${ssh_PCs[@]} ; do
	# upload test
	echo "Testing upload to $ssh_PC..."
	up_speed=`scp -v $test_file $ssh_PC:$test_file 2>&1 | \
	  grep "Bytes per second" | \
	  sed "s/^[^0-9]*\([0-9.]*\)[^0-9]*\([0-9.]*\).*$/\1/g"`
	up_speed=`echo "($up_speed*0.0009765625*100.0+0.5)/1*0.01" | bc`

	# download test
	echo "Testing download from $ssh_PC..."
	down_speed=`scp -v $ssh_PC:$test_file $test_file 2>&1 | \
	  grep "Bytes per second" | \
	  sed "s/^[^0-9]*\([0-9.]*\)[^0-9]*\([0-9.]*\).*$/\2/g"`
	down_speed=`echo "($down_speed*0.0009765625*100.0+0.5)/1*0.01" | bc`

	# clean up
	echo "Removing test file on $ssh_PC..."
	`ssh $ssh_PC "rm $test_file"`
	uplink_PCs+=($up_speed)
	downlink_PCs+=($down_speed)
done
echo "Removing test file locally..."
`rm $test_file`

# print result
echo ""
for up in ${uplink_PCs[@]}; do
	echo "Upload speed:   $up kB/s"
done
for down in ${downlink_PCs[@]}; do
	echo "Download speed: $down kB/s"
done
