package com.pingan.pbear.udf.common

import java.net.InetAddress

import com.maxmind.geoip2.DatabaseReader
import com.pingan.pbear.common.AppConf
import com.pingan.pbear.util.HDFSTool
import org.apache.hadoop.fs.{FSDataInputStream, Path}

/**
  * Created by lovelife on 17/1/6.
  */
object GeoIp {

  @transient lazy val reader = initReader()

  /**
    * 初始化ip2city数据库，数据库文件放在hdfs目录pbear-sql/udf/conf/GeoLite2-City.mmdb
    * @return
    */
  def initReader(): DatabaseReader = {
    // 上线时这里应该改为从hdfs上读取文件
    //  val source = getClass().getClassLoader().getResourceAsStream("GeoLite2-City.mmdb")
    //    val reader = new DatabaseReader.Builder(source).build()
    val fs = HDFSTool.getFileSystem()
    val hdfsRootPath = HDFSTool.getHdfsRoot()
    val geoipPath = hdfsRootPath + AppConf.getGeoipPath
    val fds = new FSDataInputStream(fs.open(new Path(geoipPath)))
    val reader = new DatabaseReader.Builder(fds).build()
    reader
  }

  /**
    * ip转城市名
    * @param ip: ip 地址
    * @return
    */
  def ip2city(ip: String): String = {
    try {
      val ipAddress = InetAddress.getByName(ip)
      val response = reader.city(ipAddress)
      val city = response.getCity()
      city.getNames.get("zh-CN")
    } catch {
      case e: Exception => ""
    }
  }

  /**
    * ip转国家名
    * @param ip: ip 地址
    * @return
    */
  def ip2country(ip: String): String = {
    try {
      val ipAddress = InetAddress.getByName(ip)
      val response = reader.city(ipAddress)
      val country = response.getCountry
      country.getNames.get("zh-CN")
    } catch {
      case e: Exception => ""
    }
  }

  def main(args: Array[String]): Unit = {
    println(ip2city("61.135.169.121"))
    println(ip2country("61.135.169.121"))
//    println(ip2country("sdf"))
  }
}
