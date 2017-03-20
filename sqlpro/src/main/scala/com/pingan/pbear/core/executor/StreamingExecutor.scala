package com.pingan.pbear.core.executor

import org.apache.spark.SparkContext
import com.pingan.pbear.common.{AppConf, SubTask}
import com.pingan.pbear.util.{HDFSTool, StringUtil}
import kafka.serializer.StringDecoder
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by ligang on 16-12-20.
  */
// @transient 必须加上，应为sparkstreaming用checkpoint保存，SparkContext不能序列化
class StreamingExecutor(hiveCtx: HiveContext, subTasks: Array[SubTask]) extends BaseExecutor(hiveCtx, subTasks) {
  override def execute(): Int = {
    log.info("***** Running Streaming Task *****")

    val streamSource = mappedSetKv("streaming.source")
    streamSource match {
      case "kafka" => streamFromKafka(hiveCtx.sparkContext, subTasks)
      case _ => throw new Exception(s"wrong stream source: $streamSource")
    }
    0
  }

  def streamFromKafka(sc: SparkContext, subTasks: Array[SubTask]) = {
    log.info("***** Streaming Source from Kafka *****")

    val streamTask = subTasks.filter(task => task.mainTag == "create" && task.subTag == "table" && task.options("dbtype") == "kafka").head

    val option = StringUtil.parseKeyValueWithQuotes(streamTask.options("option"))
    val interval = mappedSetKv("streaming.interval").toInt
    // val ckPath = setInfo.get("streaming.checkpoint").get.toString
    // checkpoint目录：ck根目录+appname
    val appName = mappedSetKv("spark.app.name").replace(" ", "")
    val ckPath = HDFSTool.getHdfsRoot() + AppConf.getCheckpointPath + appName

    val broker = option("broker")
    val offset = option("offset")
    val topic = option("topic")
    val topicList = topic.split(",").toSet
    //    val groups = theSetInstructions("kafka.groups").toString

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> broker,
      "auto.offset.reset" -> offset,
      "group.id" -> appName,
      "serializer.class" -> "kafka.serializer.StringEncoder")

    val ssc = StreamingContext.getOrCreate(ckPath,
      setupSscWithKafka(sc, topicList, kafkaParams, ckPath, interval, subTasks)
    )

    ssc.start()
    ssc.awaitTermination()
  }

  // 默认流数据表结构,表名是stream,只有一个字段line
  case class Stream(line: String)

  def setupSscWithKafka(sc: SparkContext,
                         topics: Set[String],
                         kafkaParams: Map[String, String],
                         checkpointDir: String,
                         interval: Long,
                         subTasks: Array[SubTask]
                       )(): StreamingContext = {
    val ssc = new StreamingContext(sc, Seconds(interval))
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)
    stream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        // 调用BatchExecutor是为了代码复用，这里必须重新创建一个HiveContext，
        // 让stream表注册到同一个HiveContext中
        val hiveCtx = new HiveContext(rdd.sparkContext)
        hiveCtx.createDataFrame(rdd.map(line => Stream(line._2))).registerTempTable("stream")
        new BatchExecutor(hiveCtx, subTasks).execute()
      }
    })
    ssc.checkpoint(checkpointDir)
    ssc
  }

}
