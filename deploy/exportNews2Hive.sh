#!/usr/bin/env bash

HDFS_NEWS_PATH="/apps/hduser1402/jk_news_proc/hive_cache"
HIVE_DB="jk_news_safe"

for name in news_article_tags news_topic_tags news_stock_tags news_source
do
hive <<EOF
load data inpath '${HDFS_NEWS_PATH}/${name}' into table ${HIVE_DB}.${name} ;
EOF

if [ $? -eq 0 ]
then
    echo "${HDFS_NEWS_PATH}/${name} load success!\n"
else
    echo "${HDFS_NEWS_PATH}/${name} load failed!\n"
fi
done