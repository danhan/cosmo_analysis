#!/bin/bash

# PROGNAME=$(basename $0)

myDir=$(readlink -f $0 | xargs dirname)
pushd .


#cd ${myDir}/../

#source ./bin/envrc

USAGE="USAGE:<4/5> <file directory> snapshotId"

if [ -z "$1" ]; then
	echo "$USAGE"
	exit -1
fi

if [ ! -f "${JAVA_HOME}/bin/java" ]; then
	echo "JAVA_HOME not found."
	exit -1
fi


MYLIB=${PWD}/../bin/cosmo.jar

#echo ${MYLIB}

echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
echo "Indexing Client"
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"

echo ${MYLIB}
${JAVA_HOME}/bin/java -Xmx1500m -classpath ${MYLIB} cos.dataset.space.analysis.InsertSpacialData $* 

echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
echo "Finish"
echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"

