package com.pingan.pbear.udf.tagging

import java.io.{BufferedInputStream, ByteArrayInputStream}
import java.text.SimpleDateFormat
import java.util.Date

import com.pingan.pbear.common.AppConf
import com.pingan.pbear.model.IdfModel
import com.pingan.pbear.udf.nlp.AnsjNLP
import com.pingan.pbear.util.{JsonUtil, RedisUtil}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.Logging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.Map
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xuelinbo on 16/12/22.
  */
object NewsTaggingProc extends Logging {
  private var broadcastTagList: Broadcast[List[String]] = _
  private val maxRetryCount = 3

  private val usage =
    """
  Usage: NewsTaggingProc reqType mode (startId)
    reqType = [topic, article, stock]
    mode = [test, product]
    startId = int (optional)
    """

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      log.error(usage)
      return
    }
    val reqType = args(0)
    val mode = args(1)

    val startId = if (args.length == 3) args(2).toInt else -1

    val sparkConf = new SparkConf().setAppName("newsTaggingProc")
    val sc = new SparkContext(sparkConf)

    // init redis
    val host = AppConf.getRedisHost
    val port = AppConf.getRedisPort
    val password = AppConf.getRedisPasswd
    //暂时注释
    //RedisUtil.init(host, port, password)

    // hdfs fs
    val hdconf = sc.hadoopConfiguration
    val fs = FileSystem.get(hdconf)
    val hdfsRoot = AppConf.getHdfsRoot

    // 全局变量
    val tagFile = hdfsRoot + AppConf.getFinTagFile
    val tagList = sc.textFile(tagFile).collect().toList
    broadcastTagList = sc.broadcast(tagList)

    val initRedisFlag = hdfsRoot + "/init_redis"
    if (!fs.exists(new Path(initRedisFlag))) {
      log.info("INIT REDIS begins")
      val topNewsInit: Map[String, String] = Map[String, String]("newsId" -> "0", "milliseconds" -> "1")
      val topNewsListInit = List(topNewsInit)
      RedisUtil.set("NLP_SYS_NEWS_TOPN_TOPIC", topNewsListInit)
      RedisUtil.set("NLP_SYS_NEWS_TOPN_STOCK", topNewsListInit)
      RedisUtil.set("NLP_SYS_NEWS_TOPN_ARTICLE", topNewsListInit)
      fs.create(new Path(initRedisFlag))
    }

    //init seg dict
    AnsjNLP.loadStopWords(fs, hdfsRoot + AppConf.getStopWordFile)
    AnsjNLP.loadUserdefineWords(fs, hdfsRoot + AppConf.getUserDictFile)

    //股票所属类别字典
    //lazy val stockCategoryMap = featureExtraction.getStockCategoryMap(sc)
    //val stocks = sc.textFile(newsSettings.libraryPath + "terminology/stock.dic").collect()

    try {
      /**
        * 有三种新闻栏目, 新闻专题、单篇新闻和选股
        */
      reqType match {
        case "topic" => process(sc, "topic")
        case "article" => process(sc, "article", startId)
        case "stock" => process(sc, "stock")
        case _  => {
          log.error(s"reqType is error, $reqType")
          throw new IllegalArgumentException
        }
      }
    } catch {
      case e: Exception =>
        log.error("Error message:" + e.getMessage)
    } finally {
      sc.stop()
    }
  }

  /**
    * 这是一个从API循环访问所有topicID的结构,recentTopicID是从HDFS中读出来的暂存值，
    * 每个spark-submit任务结束，都将最后的topicID存下来,
    * latestRecentTopicID是目前API上的最新ID,
    * 从latestRecentTopicID后退一直访问到recentTopicID为止
    *
    * @param sc sparkContext
    * @param reqType 请求类型(topic, article, stock)
    *
    *
    *
    */
  def process(sc: SparkContext, reqType: String, startId: Int = -1): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)

    // 从hdfs获取处理过最新的id
    val recentRecId = NewsApiUtils.getLastRecordId(fs, reqType)
    // 通过api接口获取目前资讯库中最新的id
    // 支持两个情景: 1)默认是-1,从最新开始跑;2)支持从指定位置跑
    val latestRecId = NewsApiUtils.getPreviousRecId(reqType, startId)

    log.info(s"Start loop scan on $reqType, latestRecid is $latestRecId, recentRecId is $recentRecId")
    var currentRecId = latestRecId
    var retryCounter = maxRetryCount
    var tmpRecid = latestRecId
    while (currentRecId > recentRecId && retryCounter > 0) {
      try {
        log.info(s"loop: currentRedId is $currentRecId, recentRecId is $recentRecId")
        //tmpRecId = NewsApiUtils.getPreviousRecId(reqType, currentRecId, mode)
        val reqUrl = NewsApiUtils.makeReqUrl(reqType, currentRecId)
        val resJsonData = NewsApiUtils.getJsonByUrl(reqUrl)
        val textList = NewsApiUtils.getTextFromJson(reqType, resJsonData)

        // 获取关键词列表
        val newsKeywordsDetail = extractKeywordFromText(fs, textList, AppConf.getTopRecNum)
        val newsKeywords = newsKeywordsDetail.map(_._1).mkString(";")
        log.info(s"newsKeywords is $newsKeywords")
        log.info(s"newsKeywordsDetail is ${newsKeywordsDetail.mkString(";")}")

        // 回写关键词
        //if (reqType == "topic") {
        //  NewsApiUtils.writeTopicTags(currentRecId, newsKeywords, mode)
        //}

        //apiSetNewsTag(newsSettings, newsId = currentArticleID, infoType = 1, "theme", tagName = newsKeywords)

        // 基于关键词列表贴标签
        //val (tags, detailedTags) = taggingTextByKeywords(newsKeywordsDetails, newsSettings, featureExtraction, AllKeys, taggedKeywords)
        //log.info("write: tags to API: " + tags)
        //NewsApiUtils.apiSetNewsTag(currentTopicId, infoType = 3, "industry", tags, mode)

        val tags = ""
        val detailedTags = ""

        // 构成新闻信息Map
        val newsInfoMap = NewsApiUtils.makeNewsInfoMap(resJsonData,
                                                       newsKeywords,
                                                       newsKeywordsDetail.mkString(";"),
                                                       tags,
                                                       detailedTags,
                                                       reqType)


        log.info(s"newsInfoMap is ${newsInfoMap.toString()}")
        val hiveCachePath = AppConf.getHdfsRoot + AppConf.getHiveCachePath
        newsKeywordsTagsToHDFS(fs, hiveCachePath, newsInfoMap)

        val redisNewsInfoMap: Map[String, String] = NewsApiUtils.makeNewsInfoMap(resJsonData,
                                                              newsKeywords,
                                                              newsKeywordsDetail.mkString(";"),
                                                              tags,
                                                              detailedTags,
                                                              reqType,
                                                              true)
        log.info(s"redisNewsInfoMap is ${redisNewsInfoMap.toString()}")
        // 更新Redis
        //updateRedis(redisNewsInfoMap)
        tmpRecid = currentRecId  //TODO: 倒序进行处理,可以避免重试时重复处理
        currentRecId = NewsApiUtils.getPreviousRecId(reqType, currentRecId)
        retryCounter = maxRetryCount

      } catch {
        case e: Exception =>
          log.error(s"Exception caught in tagging $reqType, currentRecId is $currentRecId")
          log.error("Error message:" + e.getMessage)
          log.error("Sleep for 10s to continue")
          Thread sleep 10000
          retryCounter -= 1
      }
    }
    log.info(s"End loop currentRecId is $currentRecId, recentRecId is $recentRecId")
    // 全部id成功处理才更新latestRecId
    if (retryCounter == maxRetryCount) {
      NewsApiUtils.setLastRecordId(fs, latestRecId, reqType)
      //TODO 统一更新redis,避免中间多次和redis交互
    }
    fs.close()
  }


  /**
    * 将处理结果入hive
    *
    * @param fs hdfs系统句柄
    * @param path 存储路径
    * @param newsInfoMap 新闻信息map
    */
  def newsKeywordsTagsToHDFS(fs: FileSystem, path: String, newsInfoMap: Map[String, String]): Unit = {
    log.info("beggin newsTagsToHDFS")
    //新闻ID
    val newsId: String = newsInfoMap("newsId")
    // 新闻来源, 定义： "ws"--华尔街见闻,  "sina" -- 新浪
    val newsSrc = ""
    //新闻类别， 根据来源自定义 目前三种类别： topic,stock,article
    val newsType: String = newsInfoMap("newsType")
    val createdTime = newsInfoMap("createdTime")
    val title = newsInfoMap("title").replace("\n", "&#10;").replace("\r", "&#13;").replace("\t", "&#09;")
    val summary = newsInfoMap("summary").replace("\n", "&#10;").replace("\r", "&#13;").replace("\t", "&#09;")
    //原文url
    val url = newsInfoMap("url").replace("\n", "&#10;").replace("\r", "&#13;").replace("\t", "&#09;")
    val imageUrl = newsInfoMap("imageUrl").replace("\n", "&#10;").replace("\r", "&#13;").replace("\t", "&#09;")
    //原始标签
    val tags = newsInfoMap("tagsSrc").replace("\n", "&#10;").replace("\r", "&#13;").replace("\t", "&#09;")
    //原始类别
    val categories = newsInfoMap("categorySrc").replace("\n", "&#10;").replace("\r", "&#13;").replace("\t", "&#09;")
    //作者ID
    val user_id = ""
    val user_name = newsInfoMap("userName").replace("\n", "&#10;").replace("\r", "&#13;").replace("\t", "&#09;")
    //文章正文
    val content = newsInfoMap("text").replace("\n", "&#10;").replace("\r", "&#13;").replace("\t", "&#09;")
    //离线任务加工的关键字
    val bigdataKeywords = newsInfoMap("keywords").replace("\n", "&#10;").replace("\r", "&#13;").replace("\t", "&#09;")
    //离线任务加工的关键字详情
    val bigdataDetailedKeywords = newsInfoMap("detailedKeywords").replace("\n", "&#10;").replace("\r", "&#13;").replace("\t", "&#09;")
    //离线任务加工的标签
    val bigdataTags = newsInfoMap("tags").replace("\n", "&#10;").replace("\r", "&#13;").replace("\t", "&#09;").replace(";",",")
    //离线任务加工的标签详情
    val bigdatadeDetailedTags = newsInfoMap("detailedTags").replace("\n", "&#10;").replace("\r", "&#13;").replace("\t", "&#09;").replace(";",",")

    val bigdataUpdateTime: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())

    val NewsSource = newsId + "\t" + newsSrc + "\t" + newsType + "\t" + createdTime + "\t" + title + "\t" + summary + "\t" + url + "\t" + imageUrl + "\t" +
      tags + "\t" + categories + "\t" + user_id + "\t" + user_name + "\t" + content + "\n"

    val NewsKeywordsTags = newsId + "\t" + newsSrc + "\t" + newsType + "\t" + bigdataKeywords + "\t" + bigdataDetailedKeywords + "\t" +
      bigdataTags + "\t" + bigdatadeDetailedTags + "\t" + bigdataUpdateTime + "\n"

    val newsSourceFile = path + "news_source"
    val newsTagsFile = path + "news_" + newsType + "_tags"

    //追加关键字和标签到标签文件
    try {
      log.info("newsTagsfile = " + newsTagsFile + "is going to append")
      IOUtils.copyBytes(new BufferedInputStream( new ByteArrayInputStream(NewsKeywordsTags.getBytes())), fs.append(new Path(newsTagsFile)),4096,true)
      log.info(newsTagsFile + " had been append ")
    } catch {
      case e: Exception =>
        log.error(newsTagsFile + " append faild ")
        log.error("Error message:" + e.getMessage)
    }

    //追加新闻源信息到新闻源文件
    try {
      log.info("newsTagsfile = " + newsSourceFile + "is going to append")
      IOUtils.copyBytes(new BufferedInputStream( new ByteArrayInputStream(NewsSource.getBytes())), fs.append(new Path(newsSourceFile)),4096,true)
      log.info(newsSourceFile + " had been append ")
    } catch {
      case e: Exception =>
        log.error(newsSourceFile + " append faild ")
        log.error("Error message:" + e.getMessage)
    }
    log.info("newsId = " + newsId + ", newsType = " + newsType + ":  newsTagsToHDFS  had been completed  !!!")
  }

  /**
    * 将新闻信息写入Redis
    *
    * @param newsInfoMap 新闻信息
    */
  def updateRedis(newsInfoMap: Map[String, String]): Unit = {
    val newsId: Int = newsInfoMap("newsId").toDouble.toInt
    val newsType: String = newsInfoMap("newsType")
    log.info("current Id is " + newsId.toString)
    log.info("newsType is " + newsType)

    val redisNewsKey = newsType match {
      case "article" => "NLP_SYS_NEWS_TOPN_ARTICLE"
      case "stock"   => "NLP_SYS_NEWS_TOPN_STOCK"
      case "topic"   => "NLP_SYS_NEWS_TOPN_TOPIC"
    }

    val topNewsList = RedisUtil.get[List[Map[String, String]]](redisNewsKey).getOrElse(List())
    log.info(s"current content in RedisKey ${redisNewsKey} is: ${topNewsList.toString()}.")
    val topIds: List[Int] = topNewsList.map(topNews => topNews("newsId").toDouble.toInt).distinct
    log.info("distinct Ids are: " + topIds.mkString(", "))
    log.info("size of topNewsList is " + topNewsList.length)
    log.info("topNewsList is " + topNewsList.toString())
    if (!topIds.contains(newsId)) {
      val extendList = (topNewsList :+ newsInfoMap).sortBy(-_("milliseconds").toLong)
      log.info("truncate Json list")

      // val updatedList: List[Map[String, String]] = extendList.take(10)
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val curTime = new Date().getTime

      val updatedList: List[Map[String, String]] = extendList.filter(it => df.parse(it.getOrElse("createdTime", "2099-12-12 12:12:12")).getTime() <= curTime).take(10)
      RedisUtil.set(redisNewsKey, updatedList)
    }

  }

  /**
    * 通过文本提取关键词
    *
    * @param fs filesystem句柄
    * @param text 待处理文本
    * @param topN 返回top数量
    * @return List[(String, Double)]  String为关键词, Double为tf-idf值
    */
  def extractKeywordFromText(fs: FileSystem, text: String, topN: Int): List[(String, Double)] = {
    //word segment
    val tokens = AnsjNLP.ansjWordSeg(text)

    //calc tf
    val hashingTF = new HashingTF(AppConf.getTfFeatureNum)
    val tf = hashingTF.transform(tokens).compressed.asInstanceOf[SparseVector]
    //load idf model
    val idfModelPath = new Path(AppConf.getHdfsRoot + AppConf.getHdfsModelPath + "IdfModel")
    val idf = IdfModel.loadIdfModel(fs, idfModelPath)

    val tfIdf = idf.transform(tf).compressed.asInstanceOf[SparseVector]

    val tfTermPairs = tfIdf.indices.zip(tfIdf.values)

    val tokenHashMap: Map[Int, String] = tokens.map(tok => (hashingTF.indexOf(tok), tok)).toMap

    val topTermsInfo: Array[(String, Double)] = tfTermPairs.map {
      case (id, value) => {
        val token = tokenHashMap.getOrElse(id, "")
        val tfValue = tf.apply(id)
        val idfValue = idf.idf.apply(id)
        //log.info(s"Before filter, token:$token, tf:$tfValue, idf:$idfValue, tf-idf:$value")
        if (idfValue == 0.0) {
          (token, tfValue * 6.0)
        }else if (idfValue < 3.0 && broadcastTagList.value.contains(token)) {
          (token, tfValue * (idfValue + 3.0))
        }else {
          (token, value)
        }
      }
    }.filter {
      case (token, value) => token.length > 2
    }.filter{
      case (token, value) => broadcastTagList.value.contains(token)
    }.sortBy(-_._2).take(topN)

    //filter
    val tmpTag = ArrayBuffer[String]()
    topTermsInfo.foreach {
      case (tag, value) => {
        topTermsInfo.foreach {
          case (tag1, value1) => {
            if (!tmpTag.contains(tag) && !tmpTag.contains(tag1) && tag.contains(tag1) && tag != tag1) {
              tmpTag.append(tag1)
            }
          }
        }
      }
    }
    val topTags = topTermsInfo.filter(item => !tmpTag.contains(item._1)).take(5)
    topTags.toList
  }
}
