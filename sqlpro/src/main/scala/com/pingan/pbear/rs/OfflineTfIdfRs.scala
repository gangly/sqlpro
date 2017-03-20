package com.pingan.pbear.rs

import java.io.ObjectInputStream
import java.text.SimpleDateFormat

import com.typesafe.config.ConfigFactory
import com.pingan.pbear.util.HDFSTool._
import com.pingan.pbear.util.RedisUtil
import com.pingan.pbear.util.JsonUtil.toJson
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.feature._
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.Logging

import scala.collection.mutable.ArrayBuffer
import scalikejdbc._
import com.pingan.pbear.util.SetupJdbc
import com.pingan.pbear.common._
import com.pingan.pbear.udf.nlp.AnsjNLP

/**
  * Created by zhangrunqin on 16-11-18.
  *
  * 调度周期：大概每５分钟需要执行一次,计算最新新闻的TF-IDF,存入Redis
  *
  * 新闻推荐流程：
  * 1.从HDFS获取模型[此模型通过一天更新一次即可]
  * 2.对候选集合新闻(最近1天的新闻)进行分词并获取后,利用IDF模型获取标题正文TF-IDF,以及新闻时间/标签/类别
  * 3.获取用户画像数据[根据用户的访问历史生成的TF-IDF值,此值可以每天一更新,也可以实时更新(每５分钟更新一次)]
  * 4.计算用户画像TF-IDF向量与候选集向量的距离,取topN进行推荐
  *
  * 计算公式：
  * dist = w1 * TF-IDF + w2 * 新闻时间权重　+ w3 * 标签距离　+ w4 * 类别距离
  * 其中: 标签也可以抽象成一个向量表示
  */
object OfflineTfIdfRs extends Logging{
  private val appCfg = ConfigFactory.load()
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    //获取配置参数
    val master = Option(appCfg.getString("IDFRec.master")).getOrElse("local[4]")
    val appName = Option(appCfg.getString("IDFRec.appName")).getOrElse("IDFModelScheduler")
    val path = Option(appCfg.getString("IDFRec.candidateNewsPath")).getOrElse("/home/Jay/test/NLP/testdata/")
    val modelPath = Option(appCfg.getString("IDFRec.modelPath")).getOrElse("/models/idfModel")
    val numFeatures = Option(appCfg.getInt("IDF.numFeatures")).getOrElse(1048576)
    val redisHost = Option(appCfg.getString("Redis.host")).getOrElse("127.0.0.1")
    val redisPort = Option(appCfg.getInt("Redis.port")).getOrElse(6379)
    val redisPassword = Option(appCfg.getString("Redis.password")).getOrElse("123456")

    val jdbcDriver = appCfg.getString("PostgreSQL.driveClassName")
    val jdbcUrl = appCfg.getString("PostgreSQL.jdbcUrl")
    val jdbcUser = appCfg.getString("PostgreSQL.username")
    val jdbcPassword = appCfg.getString("PostgreSQL.password")


    //读取新闻候选集并进行分词
    val sc = new SparkContext(new SparkConf().setMaster(master).setAppName(appName))
    val sqlContext = new SQLContext(sc)

    //根据IDF模型计算最新新闻的tf-idf
    val df = sqlContext.read.format("json").load(path)
    val idToTfIdf = df.select("id", "title", "summary", "content","tagIds", "categoryIds", "createdTime").map(r =>
      (r.getAs[String]("id"),r.getAs[String]("title") + r.getAs[String]("summary") + r.getAs[String]("content"),
        r.getAs[String]("tagIds"), r.getAs[String]("categoryIds"), r.getAs[String]("createdTime"))).
      mapPartitions(curPartition => {
        //读取IDF模型
        val model = readIdfModel(getFileSystem(), new Path(getHdfsRoot() + modelPath))
        val hashingTF = new HashingTF(numFeatures)
        for(item <- curPartition) yield (
          item._1,
          model.transform(hashingTF.transform(AnsjNLP.ansjWordSeg(item._2))),
          item._3,
          item._4,
          item._5
          )
      }).collect()

    //TODO:将最近的新闻(一天内)向量数据存入Redis中[RS_NEWS_RECENT_ARTICLES],同时需要将文章向量数据存入PG以供UserAnalysis使用
    val newsVectors = new ArrayBuffer[NewsVector]()

    idToTfIdf.filter(n => n._1.nonEmpty).foreach(item => {
      val timeStamp = sdf.parse(item._5).getTime
      val tagIds = new ArrayBuffer[Int]()
      val categoryIds = new ArrayBuffer[Int]()
      item._3.split('|').foreach(a => if(a.nonEmpty)tagIds += a.toInt)
      item._4.split('|').foreach(a => if(a.nonEmpty)categoryIds += a.toInt)
      newsVectors += NewsVector(
        item._1.toInt,
        item._2.toSparse.size,
        item._2.toSparse.indices,
        item._2.toSparse.values,
        tagIds.toArray,
        categoryIds.toArray,
        timeStamp)
    })
    if(newsVectors.size >= 3){
      val conn = RedisUtil.init(redisHost, redisPort, redisPassword)
      conn.set("RS_NEWS_RECENT_ARTICLES", toJson(newsVectors))
      val res = conn.get[String]("RS_NEWS_RECENT_ARTICLES")
      log.info(s"\n${res.get}\n")
    }
    SetupJdbc(jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword)
    //读取recent_news_vector,如果文章id存在且created_time为当前日期,则无需更新,否则更新文章vector
    val existedNewsList = DB readOnly{implicit  session =>
      val timeDuration = 1 * 24 * 60 * 60 * 1000 //1天
      val today = System.currentTimeMillis() - timeDuration
      sql"select news_id,created_time from recent_news_vector where created_time > ${today} ".
        map(rs => (rs.int("news_id"),rs.long("created_time"))).list.apply()
    }
    println("existed News>>>>>>>>>>")
    existedNewsList.foreach(println)
    val recentNews:Seq[Seq[Any]] = newsVectors.filter(n => !existedNewsList.map(_._1).contains(n.newsId)).map(r => {
      //表中不包含当前新闻id,说明是新拉取到的新闻
      val contentVectorStr = toJson(r)
      Seq(r.newsId,r.tagIds.mkString("|"),r.categoryIds.mkString("|"),contentVectorStr,r.createDate)
    })
    println("recent News>>>>>>>>>>")
    recentNews.foreach(println)
    //批量插入数据
    DB localTx{implicit session =>
      sql"insert into recent_news_vector(news_id, tag_ids, category_ids, content_vector,created_time) values(?, ?, ?, ?, ?)".batch(recentNews:_*).apply()
    }

    sc.stop()
  }

  def readIdfModel(fs: FileSystem, idfFh: Path): IDFModel = {
    // 从磁盘读取
    val ois: ObjectInputStream = new ObjectInputStream(fs.open(idfFh))
    val idf: IDFModel = ois.readObject().asInstanceOf[IDFModel]
    ois.close()
    idf
  }
}
