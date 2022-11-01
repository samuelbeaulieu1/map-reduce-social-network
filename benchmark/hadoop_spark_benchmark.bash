#!/bin/bash
# All operations are put in silent mode by sending both 
# stdin and stdout to /dev/null
#
# Prior installation of hadoop, spark and the target dataset
# is required to run this script using other scripts provided

# Creating the input directory on hadoop for targets
hadoop/bin/hdfs dfs -mkdir target >/dev/null 2>&1

# Setting format for time command to only output real time metric
# with precision of 3
TIMEFORMAT=%3R

# Directory of target datasets
TARGET_DATASET_DIR=./targets

# Execute benchmark on each dataset in targets
for target in "$TARGET_DATASET_DIR"/*
do
	echo "EXECUTING BENCHMARKS FOR TARGET ${target} ..."
	# Clearing the target input directory
	hadoop/bin/hdfs dfs -rm -r target/* >/dev/null 2>&1
	# Copying the target on hadoop
	hadoop/bin/hdfs dfs -copyFromLocal $target target >/dev/null 2>&1

    # Total times to caculate averages
	total_hadoop=0
	total_spark=0
    
    # Executing 3 times for each dataset
	for i in 1 2 3
	do
		# Executing WordCount on hadoop and capturing the real time metric
		hadoop_time=$(time (hadoop/bin/hadoop jar hadoop/wc.jar WordCount target target_output >/dev/null 2>&1) 2>&1)
		total_hadoop=$(bc <<<"scale=3; $total_hadoop+$hadoop_time")

		# Deleting the output since we don't really need it
		hadoop/bin/hdfs dfs -rm -r target_output >/dev/null 2>&1
		
		# Executing WordCount on Spark and capturing the real time metric again
		spark_time=$(time (spark/bin/spark-submit spark/examples/src/main/python/wordcount.py $target >/dev/null 2>&1) 2>&1)
		total_spark=$(bc <<<"scale=3; $total_spark+$spark_time")
		
		echo "ITERATION($i); HADOOP: ${hadoop_time}s, SPARK: ${spark_time}s"
	done

    # Averages for the dataset
	average_hadoop=$(bc <<<"scale=3; $total_hadoop/3")
	average_spark=$(bc <<<"scale=3; $total_spark/3")
	echo "AVERAGES; HADOOP: ${average_hadoop}s, SPARK: ${average_spark}s"
done
