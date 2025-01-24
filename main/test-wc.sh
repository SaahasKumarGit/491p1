#!/bin/bash
output="/tmp/824-proj1-$UID"
rm -rf diff.out mr-testout.sorted $output
mkdir $output
go run main.go shakespeare.txt 5 3
sort -k2n -k1 mr-testout > mr-testout.sorted
sort -k2n -k1 $output/mrtmp.shakespeare.txt | tail -1002 | diff - mr-testout.sorted > diff.out
if [ -s diff.out ]
then
echo "Failed test. Output should be as in mr-testout.sorted. Your output differs as follows (from diff.out):"
  cat diff.out
else
  echo "Passed test"
fi
