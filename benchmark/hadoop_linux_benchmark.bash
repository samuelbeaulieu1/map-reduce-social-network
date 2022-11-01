#!/bin/bash
# All operations are put in silent mode by sending both 
# stdin and stdout to /dev/null
#
# Prior installation of hadoop is required to run this script
# using other scripts provided

# Creating the input directory on hadoop and copying the dataset on it
hadoop/bin/hdfs dfs -mkdir input >/dev/null 2>&1
hadoop/bin/hdfs dfs -copyFromLocal pg4300.txt input >/dev/null 2>&1

# Setting format for time command to only output real time metric
TIMEFORMAT=%R

# Executing WordCount on hadoop and capturing the real time metric
hadoop_time=$(time (hadoop/bin/hadoop jar hadoop/wc.jar WordCount input output >/dev/null 2>&1) 2>&1)

# Deleting the output since we don't really need it
hadoop/bin/hdfs dfs -rm -r output >/dev/null 2>&1

# Executing WordCount on Linux and capturing the real time metric again
linux_time=$(time (cat pg4300.txt | tr ' ' '\n' | sort | uniq -c >/dev/null 2>&1) 2>&1)

echo "HADOOP EXECUTION TIME: ${hadoop_time}s"
echo "LINUX EXECUTION TIME: ${linux_time}s"