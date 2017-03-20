#! /bin/bash

hdfsHost=$1
reqType=$2


HADOOP_BIN="/appcom/hadoop/bin"
HDFS_ROOT_PATH="/apps/hduser1402/jk_news_proc"

${HADOOP_BIN}/hadoop fs -rmr ${HDFS_ROOT_PATH}/hive_cache/*

${HADOOP_BIN}/hadoop fs -touchz ${HDFS_ROOT_PATH}/hive_cache/news_source
${HADOOP_BIN}/hadoop fs -touchz ${HDFS_ROOT_PATH}/hive_cache/news_topic_tags
${HADOOP_BIN}/hadoop fs -touchz ${HDFS_ROOT_PATH}/hive_cache/news_article_tags
${HADOOP_BIN}/hadoop fs -touchz ${HDFS_ROOT_PATH}/hive_cache/news_stock_tags

echo "news tagging [$reqType] proc start...........\n"

spark-submit --master yarn-cluster \
--queue root.queue_1402_01 \
--driver-memory 4g \
--executor-memory 4g \
--class com.pingan.pbear.tagging.NewsTaggingProc \
${hdfsHost}/apps/hduser1402/jk_news_proc/pbear.news-1.0-SNAPSHOT-jar-with-dependencies.jar \
$reqType

echo "news tagging [$reqType] proc end...........\n"

echo "export to hive start...........\n"
sh exportNews2Hive.sh
echo "export to hive end...........\n"
