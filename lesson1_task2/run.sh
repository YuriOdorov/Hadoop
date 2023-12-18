#! /usr/bin/env bash

OUT_DIR="Stop_oyv"
NUM_REDUCERS=8

hdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.job.name="Streaming wordCount" \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper.py,reducer.py \
    -mapper "python mapper.py" \
    -reducer "python3 reducer.py" \
    -input /data/wiki/en_articles \
    -output ${OUT_DIR} > /dev/null

IN_DIR="Stop_oyv"
OUT_DIR="Stop_oyv2"
NUM_REDUCERS=1
# Remove previous results
hdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.job.name="Streaming wordCount" \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper3.py,reducer3.py \
    -mapper "python3 mapper3.py" \
    -reducer "python3 reducer3.py" \
    -input ${IN_DIR} \
    -output ${OUT_DIR} > /dev/null

# Checking result
hdfs dfs -cat ${OUT_DIR}/part-00000 | head


