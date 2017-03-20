package com.pingan.pbear.common

import com.typesafe.config.ConfigFactory

/**
  * Created by xuelinbo on 16/12/15.
  */
object AppConf {
  private val appConf = ConfigFactory.load("application.conf")

  private val defaultTfFeatureNum = 1048576
  private val defaultIdfMinDocFreq = 2

  /**
    * 获取运行环境模式(debug, test, product)
    * 默认为debug
    * @return
    */
  def getEnvMode = Option(appConf.getString("envmode")).getOrElse("debug")

  /**
    * 获取tf-feature哈希桶数量
    * @return tfFeatureNum
    */
  def getTfFeatureNum: Int = Option(appConf.getInt("tfFeatureNum")).getOrElse(defaultTfFeatureNum)

  /**
    * 获取文档频率最小值
    * @return idfMinDocFreq
    */
  def getIdfMinDocFreq: Int = Option(appConf.getInt("idfMinDocFreq")).getOrElse(defaultIdfMinDocFreq)

  /**
    * 获取hdfs文件系统项目根目录
    * @return hdfsRoot
    */
  def getHdfsRoot: String = Option(appConf.getString("hdfsRoot")).getOrElse("/user/hduser1402/jk_news_proc")

  /**
    * 获取华尔街资讯hdfs存放地址
    * @return hdfspath
    */
  def getTrainDataPath: String = Option(appConf.getString("hdfTrainDataPath")).getOrElse("")

  /**
    * 获取停用词外部词典
    * @return stopWordFile
    */
  def getStopWordFile: String = Option(appConf.getString("hdfStopWordFile")).getOrElse("")

  /**
    * 获取用户自定义切词词典
    * @return userDefDictFile
    */
  def getUserDictFile: String = Option(appConf.getString("hdfUserDictFile")).getOrElse("")

  /**
    * 获取财经资讯标签词典
    * @return finTagFile
    */
  def getFinTagFile: String = Option(appConf.getString("hdfFinTagFile")).getOrElse("")

  /**
    * 获取模型存放hdfs的相对路径
    * @return
    */
  def getHdfsModelPath: String = Option(appConf.getString("hdfModelPath")).getOrElse("")

  /**
    * 获取idfdebug开关
    * @return
    */
  def getIdfDebug: Boolean = Option(appConf.getBoolean("idfDebug")).getOrElse(false)

  /**
    * 获取hive数据存储路径
    * @return
    */
  def getHiveCachePath: String = Option(appConf.getString("hiveCachePath")).getOrElse("")

  /**
    * 获取上一次处理topic记录id
    * @return
    */
  def getLastTopicRecIdFile: String = Option(appConf.getString("lastTopicRecId")).getOrElse("")

  /**
    * 获取上一次处理的article记录id
    * @return
    */
  def getLastArticleRecIdFile: String = Option(appConf.getString("lastArticleRecId")).getOrElse("")

  /**
    * 获取上一次处理stock记录id
    * @return
    */
  def getLastStockRecIdFile: String = Option(appConf.getString("lastStockRecId")).getOrElse("")

  /**
    * 获取top数量
    * @return
    */
  def getTopRecNum: Int = Option(appConf.getInt("topN")).getOrElse(10)

  /**
    * 获取专题列表api url
    * @return
    */
  def getTopicListUrl: String = Option(appConf.getString("serverConf.newsApi.topicListUrl")).getOrElse("")

  /**
    * 获取专题内容API url
    * @return
    */
  def getTopicContentUrl: String = Option(appConf.getString("serverConf.newsApi.topicContentUrl")).getOrElse("")

  /**
    * 获取文章列表api url
    * @return
    */
  def getArticleListUrl: String = Option(appConf.getString("serverConf.newsApi.articleListUrl")).getOrElse("")

  /**
    * 获取文章内容api url
    * @return
    */
  def getArticleContentUrl: String = Option(appConf.getString("serverConf.newsApi.articleContentUrl")).getOrElse("")

  /**
    * 获取选股宝列表api url
    * @return
    */
  def getStockListUrl: String = Option(appConf.getString("serverConf.newsApi.stockListUrl")).getOrElse("")

  /**
    * 获取选股宝文章内容api url
    * @return
    */
  def getStockContentUrl: String = Option(appConf.getString("serverConf.newsApi.stockContentUrl")).getOrElse("")

  /**
    * 设置topic标签api
    * @return
    */
  def getSetTopicTagUrl: String = Option(appConf.getString("serverConf.newsApi.setTopicTagUrl")).getOrElse("")

  /**
    * 设置资讯标签api
    * @return
    */
  def getSetNewsTagUrl: String = Option(appConf.getString("serverConf.newsApi.setNewsTagUrl")).getOrElse("")

  /**
    * 获取redis的host
    * @return
    */
  def getRedisHost: String = Option(appConf.getString("serverConf.redis.host")).getOrElse("")

  /**
    * 获取redis的port
    * @return
    */
  def getRedisPort: Int = Option(appConf.getInt("serverConf.redis.port")).getOrElse(0)

  /**
    * 获取redis的password
    * @return
    */
  def getRedisPasswd: String = Option(appConf.getString("serverConf.redis.password")).getOrElse("")

  /**
    * 获取hadoop配置文件目录
    * @return
    */
  def getHadoopConfPath: String = Option(appConf.getString("hadoopConfPath")).getOrElse("")

  /**
    * 获取sql任务目录
    * @return
    */
  def getTasksHdfsPath = Option(appConf.getString("hdfspath.tasks")).getOrElse("")

  /**
    * 获取udf jar放置目录
    * @return
    */
  def getJarsHdfsPath = Option(appConf.getString("hdfspath.jars")).getOrElse("")

  /**
    * 获取geoip数据库文件路径
    * @return
    */
  def getGeoipPath: String = Option(appConf.getString("hdfspath.geoip")).getOrElse("")

  /**
    * 获取streaming checkpoint根目录
    * @return
    */
  def getCheckpointPath = Option(appConf.getString("hdfspath.checkpoint")).getOrElse("")

  /**
    * 获取外部数据库信息的api url
    * @return
    */
  def getDbApiUrl = Option(appConf.getString("dbapiurl")).getOrElse("")

  /************************/
  // 下面这些获取配置方法只能在debug模式和dev模式下调用，切记
  def getLocalDbUrl = appConf.getString("localDb.url")
  def getLoaclDbUser = appConf.getString("localDb.user")
  def getLocalDbPassword = appConf.getString("localDb.password")
  /*************************/

  def getShowDataFrameEnable = Option(appConf.getBoolean("showDataframe.enable")).getOrElse(true)
  def getShowDataFrameRows = Option(appConf.getInt("showDataframe.rows")).getOrElse(10)


}
