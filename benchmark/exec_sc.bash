#!/bin/bash

BUILD=false

while getopts ib flag;
do
    case "${flag}" in
        b) BUILD=true;;
        i) INPUT="soc-LiveJournal1Adj.txt";;
    esac
done

# Compile source code
if [ $BUILD = true ]; then
    map-reduce-social-network/benchmark/build_sc.bash
fi

# Setting the dataset into input hdfs
if [[ ! -z $INPUT ]]; then
   hadoop/bin/hdfs dfs -rm -r input
   hadoop/bin/hdfs dfs -mkdir input
   hadoop/bin/hdfs dfs -copyFromLocal $INPUT  input
fi

# Remove output from previous executions
rm -rf output
# Execute the script on 
hadoop/bin/hadoop jar sn.jar SocialNetwork input output
