#!/bin/bash

# PROGNAME$(basename $0)

myDir=$(readlink -f $0 | xargs dirname)

conditions="
left_join
"
# cold run how many times.
export conditions

qid=2

for j in {1..1}; do
echo "********$j Times*********************************\n"

for i in $conditions; do
echo "*******$qid*query client  for schema1**$i****"
${myDir}/query-client-s1.sh 0 $qid $i
echo "*******$qid*query client for schema2**$i****"
${myDir}/query-client-s2.sh 0 $qid $i
echo "*******$qid*query client  for schema1*with coprocessor*$i****"
${myDir}/query-client-s1.sh 1 $qid $i
echo "*******$qid*query client for schema2**with coprocessor $i****"
${myDir}/query-client-s2.sh 1 $qid $i
done

done # done with many times

echo "========================================================"
echo "==================Experiment End ======================="
echo "========================================================"
