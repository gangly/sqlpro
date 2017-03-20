package com.pingan.pbear.util

import java.io.BufferedInputStream

import com.alibaba.fastjson.JSON
import com.pingan.pbear.common.AppConf
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhangrunqin on 16-12-1.
  */
object FuncsUtil {

  def getListFromConfig(config: Config, column: String):List[String] = {
    config.getStringList(column).flatMap(s => s.split(",")).toList
  }

  /**
    * 从HDFS读取指定配置文件
    * @param filePath 文件路径
    * @return
    */
  def loadConfigFromHdfs(filePath: String): Config ={
    val fs = HDFSTool.getFileSystem()
    val fullPath = if(filePath.startsWith("hdfs://")) filePath else HDFSTool.getHdfsRoot() + filePath
    val file = fs.open(new Path(fullPath))
    val is: BufferedInputStream = new BufferedInputStream(file, 12 * 1024)
    var tmp = 0
    val strBuild: StringBuilder = new StringBuilder()
    val byteArr = new Array[Byte](12*1024)
    while((is.available() > 0) && tmp != -1 ){
      tmp = is.read(byteArr)
      strBuild.append(new String(byteArr, 0 , tmp))

    }
    file.close()
    is.close()
    ConfigFactory.parseString(strBuild.toString)
  }

  /**
    * 从HDFS加载用户字典
    * @param dictPath 字典路径,会读取所有以.dic结尾的文件进行解析
    * @return
    */
  def loadUserdefineWordsFromHdfs(dictPath: String):Seq[(String, String, Int)] ={
    val res = new ArrayBuffer[(String, String, Int)]()
    val fs = HDFSTool.getFileSystem()
    val fullPath = if(dictPath.startsWith("hdfs://")) dictPath else HDFSTool.getHdfsRoot() + dictPath
    val fsStatus = fs.getFileStatus(new Path(fullPath))
    val files = HDFSTool.recursiveListFiles(fsStatus, fs)
    files.filter(_.getName.endsWith(".dic")).foreach(f => {
      val file = fs.open(f)
      val is = new BufferedInputStream(file, 12 * 1024)
      var tmp = 0
      val strBuild: StringBuilder = new StringBuilder()
      val byteArr = new Array[Byte](12*1024)
      while((is.available() > 0) && tmp != -1 ){
        tmp = is.read(byteArr)
        strBuild.append(new String(byteArr, 0 , tmp))

      }
      file.close()
      is.close()
      strBuild.toString.split("\n").foreach(line => {
        if(line.trim.nonEmpty){
          val columns = line.split("\t")
          if(columns.length == 3){
            res += ((columns(0), columns(1), columns(2).toInt))
          }
        }
      })
      println(strBuild.toString())
      strBuild.clear()
    })
    res
  }

  /**
    * 从hdfs读取文件内容
    * @param filePath hdfs文件绝对路径
    * @return
    */
  def readHdfsFile(filePath: String): String = {
    val fs = HDFSTool.getFileSystem()
    val file = fs.open(new Path(filePath))
    val is = new BufferedInputStream(file, 12 * 1024)
    var tmp = 0
    val strBuild: StringBuilder = new StringBuilder()
    val byteArr = new Array[Byte](12*1024)
    while((is.available() > 0) && tmp != -1 ){
      tmp = is.read(byteArr)
      strBuild.append(new String(byteArr, 0 , tmp))
    }
    file.close()
    is.close()
    strBuild.toString
  }

  /**
    * 从hdfs读取文件
    * @param filePath hdfs文件相对路径
    * @return
    */
  def readFileFromHdfs(filePath: String): String = {
    val fullPath = if(filePath.startsWith("hdfs://")) filePath else HDFSTool.getHdfsRoot() + filePath
    readHdfsFile(fullPath)
  }


  /**
    * 根据配置字符串获取数据库连接信息
    * @param connStr 调度平台注册的字符串
    * @return
    */
  def connStrToConnParams(connStr: String):Map[String, String] = {
    if (List("debug", "dev").contains(AppConf.getEnvMode)) {
      if (connStr.toLowerCase.contains("_pg")) {
        Map("user" -> AppConf.getLoaclDbUser,
            "password" -> AppConf.getLocalDbPassword,
            "url" -> AppConf.getLocalDbUrl)
      } else Map()
    } else {
      val apiurl = AppConf.getDbApiUrl + connStr
      try {
        val content = HttpRequestUtil.httpGet(apiurl)
        val dbinfo = JSON.parseObject(content)
        // 数据库信息是加密的，需要解密
        Map("url" -> AESUtil.decrypt(dbinfo.getString("jdbcUrl")),
          "user" -> AESUtil.decrypt(dbinfo.getString("dbUsername")),
          "password" -> AESUtil.decrypt(dbinfo.getString("dbPassword"))
        )
      } catch {
        case e:Exception => Map()
      }
    }
  }
}
