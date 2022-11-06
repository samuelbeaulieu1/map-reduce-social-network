#!/bin/sh

# Update packages & Install java
sudo apt-get update -y
sudo apt-get install openjdk-8-jdk wget -y

# Download hadoop & spark
wget https://dlcdn.apache.org/hadoop/common/stable/hadoop-3.3.4.tar.gz
wget https://dlcdn.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz

# Decompress hadoop downloaded file to a hadoop folder
tar -zxvf hadoop-3.3.4.tar.gz
mv hadoop-3.3.4 hadoop

# Define parameters in hadoop-env.sh
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> hadoop/etc/hadoop/hadoop-env.sh
echo 'export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar' >> hadoop/etc/hadoop/hadoop-env.sh

# Compile WordCount
cd hadoop
bin/hadoop com.sun.tools.javac.Main ../map-reduce-social-network/app/WordCount.java 
jar cf wc.jar WordCount*.class

cd ..

# Decompress hadoop downloaded file to a spark folder
tar -zxvf spark-3.3.1-bin-hadoop3.tgz
mv spark-3.3.1-bin-hadoop3 spark

# Create targets folder and download the dataset
mkdir targets
cd targets

wget https://tinyurl.com/2h6a75nk
wget https://tinyurl.com/4vxdw3pa
wget https://tinyurl.com/datumz6m
wget https://tinyurl.com/dybs9bnk
wget https://tinyurl.com/j4j4xdw6
wget https://tinyurl.com/weh83uyn
wget https://tinyurl.com/vwvram8
wget https://tinyurl.com/ym8s5fm4
wget https://tinyurl.com/kh9excea

cd ..

# Download and decompress pg4300.txt.gz
curl https://www.gutenberg.org/cache/epub/4300/pg4300.txt --output pg4300.txt.gz
gzip -d pg4300.txt.gz