#!/usr/bin/env bash

HADOOP_BIN="/appcom/hadoop/bin"
HDFS_ROOT_PATH="/apps/hduser1402/jk_news_proc"

###########################
## name: package
## func: 打包需要的jar,dict等资源
###########################
function package() {
    jar_path=`pwd`"/../news/target"
    data_path=`pwd`"/../conf"
    mkdir tmp
    cd tmp
    cp ${jar_path}/pbear.news-1.0-SNAPSHOT-jar-with-dependencies.jar  .
    cp ${data_path}/dicts.tar.gz ${data_path}/idfModel.tar.gz .
    cp ../exportNews2Hive.sh .
    cp ../runNewsTagging.sh .
    cp ../runNewsTagging_init.sh .

    tar -zcf newsTagPack.tar.gz  pbear.news-1.0-SNAPSHOT-jar-with-dependencies.jar  dicts.tar.gz idfModel.tar.gz exportNews2Hive.sh runNewsTagging.sh runNewsTagging_init.sh

    mv newsTagPack.tar.gz ..
    cd -
    rm -fr tmp
}
package
