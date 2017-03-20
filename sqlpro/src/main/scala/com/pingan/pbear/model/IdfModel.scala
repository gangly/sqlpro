package com.pingan.pbear.model

import java.io.{ObjectInputStream, ObjectOutputStream}

import com.pingan.pbear.common.AppConf
import com.pingan.pbear.udf.nlp.AnsjNLP
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.feature._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import com.pingan.pbear.util.HDFSTool.{getFileSystem, getHdfsRoot}

/**
  * Created by zhangrunqin on 16-11-29.
  *
  * 配置文件:默认使用/conf/application.conf
  *
  * idf建模:根据指定数据源进行IDF建模,然后将IDF模型存入指定HDFS位置
  *
  * 使用: com.pingan.pbear.model.IdfModel /conf/yourconfig.conf
  */
object IdfModel extends Logging{

  def main(args: Array[String]): Unit = {
    log.info("\nIDF modeling beginning......\n")

    val sc = new SparkContext(new SparkConf().setAppName("IdfModel").setMaster("local[2]"))
    val sqlContext = new SQLContext(sc)

    val hdConf = sc.hadoopConfiguration
    val fs = FileSystem.get(hdConf)
    val hdfsRoot = AppConf.getHdfsRoot

    AnsjNLP.loadStopWords(fs, hdfsRoot + AppConf.getStopWordFile)
    AnsjNLP.loadUserdefineWords(fs, hdfsRoot + AppConf.getUserDictFile)
    val doc1 = "500彩票网,据世卫组织统计，当今超过1400万艾滋病毒感染者仍然对自身状况一无所知，其中的许多人是艾滋病毒感染的高风险人群，他们往往很难获得现有检测服务,澳大利亚失业率消费者情绪指数,"

    //读取新闻数据并分词
    val df = sqlContext.read.format("json").load(hdfsRoot + AppConf.getTrainDataPath)
    val docsTokenize = df.select("title", "content").map(r => r.mkString(" ")).
      mapPartitions(curPartition => {
        for(item <- curPartition) yield AnsjNLP.ansjWordSeg(item)
      })
    log.info("docsTokenize size = " + docsTokenize.count)

    //计算idf,可以将idf模型持久化,每日更新一次idf模型即可
    val hashingTF = new HashingTF(AppConf.getTfFeatureNum)
    val tf = hashingTF.transform(docsTokenize).map(token => token.compressed)
    val idfModel = new IDF(AppConf.getIdfMinDocFreq).fit(tf)

    //将模型写入HDFS
    saveIdfModel(idfModel, fs, new Path(hdfsRoot + AppConf.getHdfsModelPath + "IdfModel"))

    log.info("\nIDF modeling done......\n")
    sc.stop
  }

  /**
    * 写IDF文件
    *
    * @param idf IDFModel对象
    * @param fs 文件系统对象
    * @param idfPath idf存储路径
    */
  def saveIdfModel(idf: IDFModel, fs: FileSystem, idfPath: Path): Unit = {
    // 缓存到磁盘
    val oos: ObjectOutputStream = new ObjectOutputStream(fs.create(idfPath))
    oos.writeObject(idf)
    oos.close
    log.info("\n串行化idf成功,存入HDFS\n")
  }

  /**
    * 读取idf文件
    * @param fs
    * @param idfPath
    * @return
    */
  def loadIdfModel(fs: FileSystem, idfPath: Path): IDFModel = {
    val ois: ObjectInputStream = new ObjectInputStream(fs.open(idfPath))
    val idf: IDFModel = ois.readObject.asInstanceOf[IDFModel]
    ois.close
    idf
  }
}
