package com.pingan.pbear.rs

import java.text.SimpleDateFormat

import com.pingan.pbear.common._
import com.pingan.pbear.util.JsonUtil._
import com.pingan.pbear.util.SetupJdbc
import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhangrunqin on 16-11-19.
  * 用户行为分析：使用SparkStreaming获取用户点击数据并进行用户行为向量计算.以５分钟为时间窗口.
  * 1.获取当前５分钟内用户点击数据.
  * 2.从数据库中读取所有用户的行为数据.
  * 3.以TF－IDF为基础,计算用户的偏好向量,并更新数据库.
  *
  * tail -n 0 -f   /www/nh-nginx02/access.log  | bin/kafka-console-producer.sh
  * --broker-list 192.168.1.1:9092 --topic sb-nginx03
  */
object UserAnalysis extends Logging{
  private val appCfg = ConfigFactory.load()
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    log.info("user click data processor started......")
    //参数解析
    val modelPath = Option(appCfg.getString("IDFRec.modelPath")).getOrElse("/models/idfModel")
    val numFeatures = Option(appCfg.getInt("IDF.numFeatures")).getOrElse(1048576)
    val master = Option(appCfg.getString("IDFRec.master")).getOrElse("local[4]")
    val appName = Option(appCfg.getString("IDFRec.appName")).getOrElse("UserAnalysis")

    val interval = appCfg.getInt("Streaming.interval")
    val ckFile = appCfg.getString("Streaming.checkpointPath")
    val topics = appCfg.getString("Kafka.topics")
    val topicList = topics.split(",").toSet
    val brokers = appCfg.getString("Kafka.brokers")
    val offset = appCfg.getString("Kafka.offset")
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> offset,
      "serializer.class" -> "kafka.serializer.StringEncoder")
    //PG库连接信息
    val jdbcDriver = appCfg.getString("PostgreSQL.driveClassName")
    val jdbcUrl = appCfg.getString("PostgreSQL.jdbcUrl")
    val jdbcUser = appCfg.getString("PostgreSQL.username")
    val jdbcPassword = appCfg.getString("PostgreSQL.password")

    val ssc = StreamingContext.getOrCreate(ckFile,
      setupSsc(topicList, kafkaParams, ckFile, interval,
        master, appName,
        modelPath, numFeatures ,
        jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword) _
    )

    ssc.start()
    ssc.awaitTermination()
  }

  def setupSsc(
                topics: Set[String],
                kafkaParams: Map[String, String],
                checkpointDir: String,
                interval:Long,
                master: String,
                appName: String,
                modelPath: String,
                numFeatures: Int,
                jdbcDriver: String,
                jdbcUrl: String,
                jdbcUser: String,
                jdbcPassword: String
              )(): StreamingContext = {
    val ssc = new StreamingContext(new SparkConf().setAppName(appName).setMaster(master), Seconds(interval))

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
    //解析用户访问日志:从kafka获取用户最近的访问记录
    //实时更新用户的偏好向量表:user_vector,以及用户标签表:user_tags,以实现实时推荐
    //单机版
    stream.map(line => {
      val columns = logParser(line._2)
      (columns.getOrElse("clientNo", "-1").toInt, columns.getOrElse("newsId","-1").toInt)
    }).groupByKey().foreachRDD(rdd => {
      //集群模式
      //rdd.foreachPartition(curPartition => )

      //单机模式,方便测试
      val res = rdd.collect()
      SetupJdbc(jdbcDriver, jdbcUrl, jdbcUser, jdbcPassword)
      //加载文章TF-IDF,标签/类别等信息[从PG数据库中加载]
      log.info("\n从recent_news_vector加载文章信息\n")
      val newsInfoList = DB readOnly {implicit session =>
        sql"select news_id,tag_ids,category_ids,content_vector from recent_news_vector".map(rs =>
          (rs.int("news_id"),rs.string("tag_ids"),rs.string("category_ids"),rs.string("content_vector"))).list.apply
      }
      newsInfoList.foreach(println)

      res.foreach(user => {
        val clientNo = user._1.toString
        //根据用户当前点击的文章id,从库中获取文章的详细信息
        val curUserClickedNews = newsInfoList.filter(n => user._2.contains(n._1))

        println(s"\n更新用户点击历史,当前用户:${user._1.toString}\n")
        //更新用户点击历史表click_history
        val clickTime = System.currentTimeMillis
        val userClickHistory:Seq[Seq[Any]] = curUserClickedNews.map(a => Seq(clientNo, a._1, clickTime)).distinct
        if(userClickHistory.nonEmpty){
          DB localTx{implicit session =>
            sql"insert into click_history(client_no, news_id, click_date) values(?, ?, ?)".batch(userClickHistory:_*).apply()
          }
        }

        userClickHistory.foreach(println)
        //读取用户TF-IDF
        val userVectorInfo = DB.readOnly{implicit session =>
          sql"select content_vector from  user_vector where client_no = $clientNo ".map(rs => rs.string("content_vector")).list.apply
        }

        log.info(s"\n已有用户向量是否为空：${userVectorInfo.isEmpty}\n")
        log.info(s"\n重新计算用户向量,当前用户:${user._1.toString}\n")

        //TODO:后续可能需要加入阈值过滤以避免用户偏好向量过大
        if(userVectorInfo.nonEmpty){
          //更新
          val oldVector = fromJson[UserVector](userVectorInfo.head)
          val newVector = computeUserPreferVector(Option(oldVector), curUserClickedNews, numFeatures, user._1)
          if(newVector.nonEmpty && newVector != ""){
            DB.localTx{implicit session =>
              sql"update user_vector set content_vector = $newVector where client_no = $clientNo".update.apply
            }
          }
        }else{
          //插入
          val newVector = computeUserPreferVector(None, curUserClickedNews, numFeatures, user._1)
          if(newVector.nonEmpty && newVector != ""){
            DB.localTx{implicit session =>
              sql"insert into user_vector(client_no, content_vector) values($clientNo, $newVector) ".update.apply()
            }
          }
        }

        log.info(s"\n重新计算用户标签,当前用户:${user._1.toString}\n")
        //读取用户标签偏好
        val userTagsInfo = DB.readOnly{implicit session =>
          sql"select tag_id, read_count,tag_type from user_tags where client_no = $clientNo".
            map(rs => (rs.int("tag_id"),rs.int("read_count"),rs.int("tag_type"))).list.apply
        }
        userTagsInfo.foreach(println)
        println("userTagsInfo size = " + userTagsInfo.size)
        //更新用户标签偏好,[注意:标签和分类存放在同一张表内,以tag_type进行区分,0:表示标签,1:表示分类]
        val tagIds = new ArrayBuffer[Int]()
        val categoryIds = new ArrayBuffer[Int]()

        curUserClickedNews.foreach(n => {
          if(n._2.nonEmpty){
            tagIds ++= n._2.split('|').map(_.toInt)
          }
          if(n._3.nonEmpty){
            categoryIds ++= n._3.split('|').map(_.toInt)
          }
        })
        //统计当前用户所阅读的文章中每个标签出现的次数
        val tagStats = tagIds.distinct.map(id => (clientNo, id, tagIds.count(a => a == id), 0))
        val categoryStats = categoryIds.distinct.map(id => (clientNo, id, categoryIds.count(a => a == id), 1))

        print("tagStats>>>>>")
        tagStats.foreach(println)

        print("categoryStats>>>>>")
        categoryStats.foreach(println)

        val userTagIdTypes = userTagsInfo.map(u => u._1.toString + "|" + u._3.toString).distinct
        //已经存在的标签
        val existedTags = tagStats.filter(t => userTagIdTypes.contains(t._2.toString + "|0")).
          map(t => (t._1, t._2, t._3 + userTagsInfo.filter(a => (a._1 == t._2) && (a._3 == 0)).head._2, 0))
        val existedCategories = categoryStats.filter(t => userTagIdTypes.contains(t._2.toString + "|1")).
          map(t => (t._1, t._2, t._3 + userTagsInfo.filter(a => (a._1 == t._2) && (a._3 == 1)).head._2, 1))

        val nonExistedTags = tagStats.filterNot(t => userTagIdTypes.contains(t._2.toString + "|0")).
          map(t => (t._1, t._2, t._3, 0))
        val nonExistedCategories = categoryStats.filterNot(t => userTagIdTypes.contains(t._2.toString + "|1")).
          map(t => (t._1, t._2, t._3, 1))
        println("existedTags>>>")
        existedTags.foreach(println)

        println("existedCategories>>>")
        existedCategories.foreach(println)

        println("nonExistedTags>>>")
        nonExistedTags.foreach(println)

        println("nonExistedCategories>>>")
        nonExistedCategories.foreach(println)
        //修改(新增)的标签/类别
        DB.localTx { implicit session =>
          if(existedTags.nonEmpty){
            existedTags.foreach(tag =>{
              sql"update user_tags set read_count = ${tag._3} where client_no= ${tag._1.toString} and tag_type = 0".update.apply
            })
          }
          if(existedCategories.nonEmpty){
            existedCategories.foreach(cat => {
              sql"update user_tags set read_count = ${cat._3} where client_no= ${cat._1.toString} and tag_type = 1".update.apply
            })
          }
          if(nonExistedTags.nonEmpty || nonExistedCategories.nonEmpty){
            sql"insert into user_tags(client_no, tag_id, read_count, tag_type) values(?, ?, ?, ?)".
              batch((nonExistedTags ++ nonExistedCategories).map(t => Seq(t._1, t._2, t._3, t._4)):_*).apply()
          }
        }
      })
    })

    ssc.checkpoint(checkpointDir)
    ssc
  }

  def logParser(line: String):Map[String, String] ={
    //[21/Nov/2016 10:49:31]"GET /api/news?id=2&uid=123 HTTP/1.1" 404 1935
    val columns = line.split(" ")
    val date = columns(0).substring(1)
    val time = columns(1).substring(0, 8)
    val url = columns(2)
    val newsId = if(url.contains("id=")) url.split("id=")(1).split('&')(0) else "-1"
    val code = columns(4)
    val clientNo = if(url.contains("uid=")) url.split("uid=")(1).split('&')(0) else "-1"
    Map("date" -> date,"time" -> time,"url" -> url,"code" -> code, "newsId" -> newsId,"clientNo" -> clientNo)
  }

  //计算用户偏好向量[稀疏向量]
  /**
    *
    * @param oldVector 用户已有偏好向量
    * @param clickedNewsList 用户最近点击过的文章详情,包括tag_ids,category_ids,content_vector等
    * @return UserVector的Json串
    */
  def computeUserPreferVector(oldVector: Option[UserVector],
                              clickedNewsList:List[(Int, String,String, String)], numFeatures: Int, clientNo: Int): String = {
    val newIndices = new ArrayBuffer[Int]()
    val newValues = new ArrayBuffer[Double]()
    if(oldVector.nonEmpty){
      newIndices ++= oldVector.get.indices
      newValues ++= oldVector.get.values
    }
    clickedNewsList.foreach(v => {
      val vector = fromJson[NewsVector](v._4)
      if(vector.indices.nonEmpty){
        newIndices ++= vector.indices
        newValues ++= vector.values
      }
    })
    if(newIndices.length == newValues.length){
      var sum = 0.0
      val t = newIndices.indices.map(i => (newIndices(i), newValues(i))).groupBy(_._1).map(kv => {
        sum = 0.0
        kv._2.foreach(v => sum += v._2)
        (kv._1, sum)
      })
      val resIndices = new ArrayBuffer[Int]()
      val resValues = new ArrayBuffer[Double]()
      t.toList.sortBy(_._1).map(a =>{
        resIndices += a._1
        resValues += "%.4f".format(a._2).toDouble
      })
      println(resIndices)
      println(resValues)
      toJson(UserVector(clientNo, numFeatures, resIndices.toArray, resValues.toArray))
    }else{
      ""
    }
  }
}
