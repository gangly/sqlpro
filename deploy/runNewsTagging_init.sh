#!/usr/bin/env bash

HADOOP_BIN="/appcom/hadoop/bin"
HDFS_ROOT_PATH="/apps/hduser1402/jk_news_proc"

##########################
## name: init
## func: 只在第一次部署时运行一次,用于创建相关目录
##########################
function init() {

    tar -zxf dicts.tar.gz
    tar -zxf idfModel.tar.gz

    # 清理相关路径
    ${HADOOP_BIN}/hadoop fs -rmr ${HDFS_ROOT_PATH}/dicts
    ${HADOOP_BIN}/hadoop fs -rmr ${HDFS_ROOT_PATH}/models
    ${HADOOP_BIN}/hadoop fs -rmr ${HDFS_ROOT_PATH}/records
    ${HADOOP_BIN}/hadoop fs -rmr ${HDFS_ROOT_PATH}/train_data
    ${HADOOP_BIN}/hadoop fs -rmr ${HDFS_ROOT_PATH}/hive_cache
    ${HADOOP_BIN}/hadoop fs -rmr ${HDFS_ROOT_PATH}/pbear.news-1.0-SNAPSHOT-jar-with-dependencies.jar


    ${HADOOP_BIN}/hadoop fs -mkdir ${HDFS_ROOT_PATH}/models
    ${HADOOP_BIN}/hadoop fs -mkdir ${HDFS_ROOT_PATH}/records
    ${HADOOP_BIN}/hadoop fs -mkdir ${HDFS_ROOT_PATH}/train_data
    ${HADOOP_BIN}/hadoop fs -mkdir ${HDFS_ROOT_PATH}/hive_cache
    ${HADOOP_BIN}/hadoop fs -touchz ${HDFS_ROOT_PATH}/hive_cache/news_source
    ${HADOOP_BIN}/hadoop fs -touchz ${HDFS_ROOT_PATH}/hive_cache/news_topic_tags
    ${HADOOP_BIN}/hadoop fs -touchz ${HDFS_ROOT_PATH}/hive_cache/news_article_tags
    ${HADOOP_BIN}/hadoop fs -touchz ${HDFS_ROOT_PATH}/hive_cache/news_stock_tags


    # 将本地词典、模型文件上传
    hadoop fs -put dicts  ${HDFS_ROOT_PATH}
    hadoop fs -put IdfModel ${HDFS_ROOT_PATH}/models
    hadoop fs -put pbear.news-1.0-SNAPSHOT-jar-with-dependencies.jar  ${HDFS_ROOT_PATH}
}

init
