/**
  * Created by xuejun & bni on 6/13/16.
  */
package com.pingan.pbear.udf.tagging

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.text.SimpleDateFormat
import java.util.Calendar

import com.pingan.pbear.common.AppConf
import com.pingan.pbear.util.HDFSTool
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.http.NameValuePair
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * 读写INFO系统API接口
  *
  * @see INFO接口文档
  */
object NewsApiUtils extends Serializable {

  private val readTimeout: Int = 50000
  private val connectTimeout: Int = 50000
  private val connectionRequestTimeout: Int = 50000
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * 获取当前RecordId前（较旧）一个Id
    *
    * @param reqType 请求类型
    * @param recordId 当前id值
    * @return Id值
    */
  def getPreviousRecId(reqType: String, recordId: Int): Int = {
    logger.info("Begin getPreviousRecId ......")
    val recIds: List[Int] = reqType match {
      case "topic" => {
        val data = "{\"limit\":1,\"lastRecordId\":" + recordId + "}"
        val url = AppConf.getTopicListUrl + data
        val topicListJSON = getJsonByUrl(url)
        val jsonInstance = topicListJSON.asInstanceOf[Option[Map[String, Map[String, List[Map[String, Any]]]]]]
        jsonInstance.map(_("data")("topicList").map(_("id"))).getOrElse(List()).map(_.toString.toDouble.intValue)
      }
      case "article" => {
        val data = "{\"limit\":1,\"lastRecordId\":" + recordId + "}"
        val url = AppConf.getArticleListUrl + data
        val newsListJSON = getJsonByUrl(url)
        val jsonInstance = newsListJSON.asInstanceOf[Option[Map[String, Map[String, List[Map[String, Any]]]]]]
        jsonInstance.map(_("data")("newsList").map(_("id"))).getOrElse(List()).map(_.toString.toDouble.intValue)
      }
      case "stock" => {
        val data = "{\"limit\":\"1\",\"lastRecordId\":" + "\"" + recordId + "\"" + "}"
        val url = AppConf.getStockListUrl + data
        val stockListJSON = getJsonByUrl(url)
        val jsonInstance = stockListJSON.asInstanceOf[Option[Map[String, List[Map[String, Any]]]]]
        jsonInstance.map(_("data").map(_("id"))).getOrElse(List()).map(_.toString.toDouble.intValue)
      }
      case _ => {
        logger.error(s"reqType is error,[$reqType]")
        throw new IllegalArgumentException
      }
    }
    val preId = if (recIds.isEmpty) -1 else recIds.head
    logger.info(s"End getPreviousRecId, id is $preId ......")
    preId
  }

  /**
    * 字符串清洗html标签等清洗
    *
    * @param str 字符串
    * @return 清洁字符串
    */
  def cleanString(str: String): String = {
    str.replaceAll("<.*?>|[\r\n]|(&nbsp)", "")
      .replaceAll("&ldquo;", "\"").replaceAll("&rdquo;", "\"").replaceAll("&[a-zA-Z]{0,20};", "")
  }


  /**
    * 获取url的json格式内容
    *
    * @param newsUrl URL地址
    * @return JSON内容
    */
  def getJsonByUrl(newsUrl: String): Option[Any] = {
    //logger.info("visit news url at " + newsUrl)
    val newsContentString = getDataFromAPI(newsUrl)
    val newsContentJSON = JSON.parseFull(newsContentString)
    newsContentJSON
  }


  /**
    * 从json中获取指定key对应的value, 兼容null情形
    * @param dataSrc
    * @param key
    * @return
    */
  def getStrValue(dataSrc: Map[String, Any], key: String): String = {
    if (dataSrc.getOrElse(key, "") != null) {
      dataSrc.getOrElse(key, "").toString
    }
    else ""
  }


  /**
    * 从json数据中解析text文本
    *
    * @param reqType
    * @param jsonData
    * @return
    */
  def getTextFromJson(reqType: String, jsonData: Option[Any]): String = {
    val text = reqType match {
      case "topic" => {
        val jsonInstance = jsonData.asInstanceOf[Option[Map[String, Map[String, List[Map[String, Any]]]]]]
        val newsIds = jsonInstance.map(_("data")("themes").map(_("id"))).getOrElse(List()).map(_.toString.toDouble.intValue)
        var newsList = new ListBuffer[String]()
        newsIds.map(
          newsId => {
            println(s"newsId: $newsId")
            val newsContent = getTextByRecId("article", newsId)
            newsList += newsContent.trim
          }
        )
        newsList.toList
        newsList.mkString("$$")
      }
      case "article" => {
        val jsonInstance = jsonData.asInstanceOf[Option[Map[String, Map[String, Any]]]]
        cleanString(getStrValue(jsonInstance.map(_("data")).get, "content"))
      }
      case "stock" => {
        val jsonInstance = jsonData.asInstanceOf[Option[Map[String, Map[String, Any]]]]
        cleanString(getStrValue(jsonInstance.map(_("data")).get, "content"))
      }
      case _ => {
        logger.error(s"reqType is error. [$reqType]")
        throw new IllegalArgumentException
      }
    }
    text
  }

  /**
    * 通过recId获取Text
    *
    * @param reqType 请求类型
    * @param recId Id值
    * @return text
    */
  def getTextByRecId(reqType: String, recId: Int): String = {
    logger.debug("begin getTextByRecId ......")
    val reqUrl = makeReqUrl(reqType, recId)
    val topicListJson = getJsonByUrl(reqUrl)
    getTextFromJson(reqType, topicListJson)
  }

  /**
    * 构造请求内容页面url
    *
    * @param reqType
    * @param recId
    * @return
    */
  def makeReqUrl(reqType: String, recId: Int): String = {
    reqType match {
      case "topic" => {
        AppConf.getTopicContentUrl + "{\"topicId\":" + recId + "}"
      }
      case "article" => {
        AppConf.getArticleContentUrl + "{\"newsId\":" + recId + "}"
      }
      case "stock" => {
        AppConf.getStockContentUrl + "{\"id\":" + "\"" + recId + "\"" + "}"
      }
      case _ => {
        logger.error(s"reqType is error. [$reqType]")
        throw new IllegalArgumentException
      }
    }
  }

  /**
    * 构建一新闻专题对应的信息Map
    *
    * @param jsonData JSON
    * @param keywords 关键词
    * @param detailedKeywords 详细关键词
    * @param tags 标签
    * @param detailedTags 详细标签
    * @param reqType 请求类型
    * @return Map对象
    */
  def makeNewsInfoMap(jsonData: Option[Any], keywords: String, detailedKeywords: String, tags: String,
                      detailedTags: String, reqType: String, isRedis: Boolean = false): Map[String, String] = {

    val jsonInstance = jsonData.asInstanceOf[Option[Map[String, Map[String, Any]]]].map(_("data")).get

    var newsId: Int = -1
    var tagsSrc: String = ""
    var categorySrc: String = ""
    var userName: String = ""
    reqType match {
      case "topic" => {
        newsId = getStrValue(jsonInstance, "id").toDouble.intValue
        tagsSrc = getStrValue(jsonInstance, "tagName")
      }
      case "article" => {
        newsId = getStrValue(jsonInstance, "newsId").toDouble.intValue
        var tmpStr = getStrValue(jsonInstance, "tag").replace("\\", "").replace("\"{", "{").replace("}\"", "}")
        //println(s"article tag str is $tmpStr")
        //println(JSON.parseFull(tmpStr).getOrElse("").toString)
        tagsSrc = if (tmpStr != "")
                    JSON.parseFull(tmpStr).asInstanceOf[Option[List[Map[String, String]]]].map(_.map(_("tagName"))).getOrElse(List()).mkString(";")
                  else ""

        tmpStr = getStrValue(jsonInstance, "category").replace("\\", "").replace("\"{", "{").replace("}\"", "}")
        //println(s"article category str is $tmpStr")
        //println(JSON.parseFull(tmpStr).getOrElse("").toString)
        categorySrc = if (tmpStr != "")
                        JSON.parseFull(tmpStr).asInstanceOf[Option[List[Map[String, String]]]].map(_.map(_("categoryName"))).getOrElse(List()).mkString(";")
                      else ""
        userName = getStrValue(jsonInstance, "createdBy")
      }
      case "stock" => {
        newsId = getStrValue(jsonInstance, "id").toDouble.intValue

        val stocksStr = getStrValue(jsonInstance, "stocks").replace("\\", "").replace("\"{", "{").replace("}\"", "}")
        tagsSrc = if (stocksStr != "")
                    JSON.parseFull(stocksStr).asInstanceOf[Option[List[Map[String, String]]]].map(_.map(_("Name"))).getOrElse(List()).mkString(";")
                  else ""

        userName = getStrValue(jsonInstance, "createdBy")
      }
      case _ => {
        logger.error(s"reqType is error. [$reqType]")
        throw new IllegalArgumentException
      }
    }

    val createdTime = getStrValue(jsonInstance, "createdTime")
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(createdTime))
    val milliseconds = cal.getTimeInMillis().toString

    val title = cleanString(getStrValue(jsonInstance, "title"))
    val summary = cleanString(getStrValue(jsonInstance, "summary"))
    val url = getStrValue(jsonInstance, "originalUrl")
    val imageUrl = getStrValue(jsonInstance, "imageUrl")

    val content = getTextFromJson(reqType, jsonData)
    val newsInfoMap = if (!isRedis) Map[String, String](
      ("newsId", newsId.toString),
      ("newsType", reqType),
      ("title", title),
      ("summary", summary),
      ("text", content),
      ("keywords", keywords),
      ("detailedKeywords", detailedKeywords),
      ("tags", tags),
      ("detailedTags", detailedTags),
      ("url", url),
      ("imageUrl", imageUrl),
      ("createdTime", createdTime),
      ("milliseconds", milliseconds),
      ("userName", userName),
      ("tagsSrc", tagsSrc),
      ("categorySrc", categorySrc)
    ) else Map[String, String](
      ("newsId", newsId.toString),
      ("newsType", reqType),
      ("title", title),
      ("summary", summary),
      ("text", ""),
      ("keywords", ""),
      ("detailedKeywords", ""),
      ("tags", ""),
      ("detailedTags", ""),
      ("url", url),
      ("imageUrl", imageUrl),
      ("createdTime", createdTime),
      ("milliseconds", milliseconds)
    )
    newsInfoMap
  }

  /**
    * 从存储中读出上次spark任务处理过的最新newsId
    *
    * @param fs hdfs filesystem
    * @param reqType 请求类型
    * @return Id值
    */
  def getLastRecordId(fs: FileSystem, reqType: String): Int = {
    logger.info(s"Begin getLastRecordId, reqType is $reqType ......")
    val hdfsRoot = AppConf.getHdfsRoot
    val recordIdFile = reqType match {
      case "topic" => {
        hdfsRoot + AppConf.getLastTopicRecIdFile
      }
      case "article" => {
        hdfsRoot + AppConf.getLastArticleRecIdFile
      }
      case "stock" => {
        hdfsRoot + AppConf.getLastStockRecIdFile
      }
      case _ => {
        logger.error(s"reqType is error. [$reqType]")
        throw new IllegalArgumentException
      }
    }
//    logger.info(recordIdFile)
    val filePath = new Path(recordIdFile)
    val lastRecordId: Int = if (fs.exists(filePath)) {
      val ois: ObjectInputStream = new ObjectInputStream(fs.open(filePath))
      //val value = ois.readObject().asInstanceOf[Int]
      val value = ois.readInt()
      ois.close()
      //sc.textFile(recordIdFile).first().toString.toDouble.intValue
      value
    } else {
      -1
    }
    logger.info(s"End getLastRecordId, reqType is $lastRecordId, id is: $lastRecordId.......")
    lastRecordId //stock 需要转为string
  }


  /**
    * spark任务结束后，在存储中存储处理过的最新newsId
    *
    * @param recId 记录的值(注意stock为string)
    * @param reqType 请求类型
    */
  def setLastRecordId(fs: FileSystem, recId: Int, reqType: String): Unit = {
    logger.info(s"Begin setLastRecordId, recid is $recId, reqType is $reqType ......")
    val hdfsRoot = AppConf.getHdfsRoot
    val recordIdFile = reqType match {
      case "topic" => {
        hdfsRoot + AppConf.getLastTopicRecIdFile
      }
      case "article" => {
        hdfsRoot + AppConf.getLastArticleRecIdFile
      }
      case "stock" => {
        hdfsRoot + AppConf.getLastStockRecIdFile
      }
      case _ => {
        logger.error(s"reqType is error. [$reqType]")
        throw new IllegalArgumentException
      }
    }
    val lastRecordIdFh = fs.create(new Path(recordIdFile), true)
    val oos: ObjectOutputStream = new ObjectOutputStream(lastRecordIdFh)
    //oos.writeObject(recId)
    oos.writeInt(recId)
    oos.close()
    logger.info(s"End setLastRecordId, recid is $recId, reqType is $reqType ......")
  }

  /**
    * 写新闻专题的关键词
    * writeTopicTags 和 apiSetNewsTag 两个写接口，分别是不同人写的，
    * 本来应该统一到apiSetNewsTag的，但现在不能统一，因为上述的这些参数分别在两个接口里得以实现
    *
    * @param topicId topic newsId
    * @param tags 写入的字符串
    */
  def writeTopicTags(topicId: Int, tags: String): Unit = {
    // logger.info("begin to write topic tags ......")
    val dataString = "{\"topicId\":" + topicId.toString + ", \"tag\":\"" + tags + "\"}"
    val param = Map("data" -> dataString, "appid" -> "10003")
    val postResult = postDataToAPI(AppConf.getSetTopicTagUrl, param, "UTF-8")
    logger.info("post results: " + postResult)
    // logger.info("completed to write topic tags")
  }

  /**
    * 写API的统一接口
    *
    * @param newsId 可以是 articleId topicId 或 stockId
    * @param infoType infoType是1 3 5，分别是新闻单篇 新闻专题和选股宝
    * @param tagType tagtype是theme=>新闻话题(关键词）、Industry=>分类标签、stock=>推荐股票
    * @param tagName 为写入的值
    */
  def apiSetNewsTag(newsId: Int, infoType: Int, tagType: String, tagName: String): Unit = {
    // logger.info("begin to set news tags via API ......")
    if (tagName == "") {
      logger.info("apiSetNewsTag return, because tagName is empty.[newsid = " + newsId + "]")
      return
    }

    val dataString = "{\"newsId\":" + newsId.toString +
      ", \"infoType\":" + infoType.toString +
      ", \"tagType\":\"" + tagType + "\"" +
      ", \"tagName\":\"" + tagName + "\"}"
    logger.info("dataString :" + dataString)
    val param = Map("data" -> dataString, "appId" -> "10002")
    val postResult = postDataToAPI(AppConf.getSetNewsTagUrl, param, "UTF-8")
    logger.info("post results: " + postResult)
    // logger.info("completed to set news tags via API ......")
  }

  /**
    * post方法实现
    *
    * @param url 写数据目的url
    * @param paramsMap post方法参数
    * @param charset 字符集
    * @return 状态
    */
  def postDataToAPI(url: String, paramsMap: Map[String, String], charset: String): String = {
    logger.info("begin postDataToAPI ......")
    if (url == null || url.isEmpty) {
      return null
    }
    val charset1 = if (charset == null) "UTF-8" else charset
    var response: CloseableHttpResponse = null
    var res: String = null
    try {
      val params: java.util.List[NameValuePair] = new java.util.ArrayList[NameValuePair]
      import scala.collection.JavaConversions._
      for (map <- paramsMap.entrySet) {
        params.add(new BasicNameValuePair(map.getKey, map.getValue))
      }
      val formEntity: UrlEncodedFormEntity = new UrlEncodedFormEntity(params, charset1)
      val httpPost: HttpPost = new HttpPost(url)
      httpPost.addHeader("Accept-Encoding", "*")
      httpPost.setEntity(formEntity)
      val configNoProxy: RequestConfig = RequestConfig.custom.setSocketTimeout(readTimeout)
        .setConnectTimeout(connectTimeout).setConnectionRequestTimeout(connectionRequestTimeout).build
      val noProxyHttpClient: CloseableHttpClient = HttpClients.custom.setDefaultRequestConfig(configNoProxy).build
      response = noProxyHttpClient.execute(httpPost)
      res = EntityUtils.toString(response.getEntity)
      if (response.getStatusLine.getStatusCode != 200) {
        throw new Exception(res)
      }
    } catch {
      case e: Exception =>
        logger.error("postDataToAPI error:" + e.getMessage)
        throw new Exception(e)
    } finally {
      if (response != null) {
        response.close()
      }
    }
    logger.info("completed postDataToAPI ......")
    res
  }

  /**
    * http get方法实现, 主要为控制超时等问题
    *
    * @param url 读数据目的url
    * @param requestMethod 请求类型
    * @return 状态
    */
  def getDataFromAPI(url: String, requestMethod: String = "GET") = {
    import java.net.{HttpURLConnection, URL}
    try {
      val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(connectTimeout)
      connection.setReadTimeout(readTimeout)
      connection.setRequestMethod(requestMethod)
      val inputStream = connection.getInputStream
      val content = Source.fromInputStream(inputStream).mkString
      if (inputStream != null) inputStream.close
      content
    } catch {
      case e: Exception =>
        logger.error("getDataFromURL error:" + e.getMessage)
        throw new Exception(e)
    }
  }


  def main(args: Array[String]): Unit = {
    val usage =
      """
      Usage: NewsApiUtils reqType method [recid]
      reqType = [topic, article, stock]
      method = [get, set]
      recid = [if method is set, necessary]
      """

    if (args.length < 2) {
      logger.error(s"args is error, \n$usage\n")
      return
    }

    val reqType = args(1) //topic, article, stock
    val method = args(2)  //get, set
    var recId = -1
    if (method == "set") {
      if (args.length < 4) {
        logger.error(s"args is error, \n$usage\n")
        return
      }
      recId = args(3).toInt
    }

    val latestRecId = getPreviousRecId(reqType, -1)
    logger.info(s"\n---------------\nlatest $reqType id is: $latestRecId\n-----------------\n")

    val fs = HDFSTool.getFileSystem()

    if (method == "get") {
      val lastRecId = getLastRecordId(fs, reqType)
      logger.info(s"\n---------------\nlast $reqType id is: $lastRecId\n-----------------\n")
    } else {
      setLastRecordId(fs, recId, reqType)
      logger.info(s"\n---------------\nset $reqType id is: $recId\n-----------------\n")
    }
//    val topicText = getTextByRecId("topic", 140, mode)
//    println(s"topic 140 text is \n $topicText")
//    val newsText = getTextByRecId("article", 12651, mode)
//    println(s"news 12651 text is \n $newsText")
//    val stockText = getTextByRecId("stock", 4520, mode)
//    println(s"stock 4520 text is \n $stockText")

  }

}

