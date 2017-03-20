#!/usr/bin/env bash

HADOOP_BIN="/appcom/hadoop/bin"
PBEARSQL_ROOT_PATH="/apps/hduser1402/pbear-sql"

${HADOOP_BIN}/hadoop fs -mkdir -p ${PBEARSQL_ROOT_PATH}/tasks/common
${HADOOP_BIN}/hadoop fs -mkdir -p ${PBEARSQL_ROOT_PATH}/jars/common
${HADOOP_BIN}/hadoop fs -mkdir -p ${PBEARSQL_ROOT_PATH}/files/common
${HADOOP_BIN}/hadoop fs -mkdir -p ${PBEARSQL_ROOT_PATH}/checkpoint
${HADOOP_BIN}/hadoop fs -mkdir -p ${PBEARSQL_ROOT_PATH}/conf