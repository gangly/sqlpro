package com.pingan.pbear.util

import com.pingan.pbear.common.AppConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.Logging

/**
  * Created by zhangrunqin on 16-11-18.
  */
object HDFSTool extends Logging{
  //hadoop集群的配置文件路径需要固定
  private val configBasePath = AppConf.getHadoopConfPath
  log.info(configBasePath)

  /**
    * 递归列出目录
 *
    * @param status
    * @param fs
    * @return
    */
  def recursiveListFiles(status: FileStatus, fs: FileSystem): Array[Path] = {
    log.info("BEGIN  recursiveListFiles ......")
    var fileArray = Array[Path]()
    try {
      val inputPath = new Path(status.getPath.toString)
      val fileStates = fs.listStatus(inputPath)
      for (subFs <- fileStates) {
        if (subFs.isDirectory) {
          fileArray = fileArray ++: recursiveListFiles(subFs, fs)
        } else if (subFs.isFile) {
          fileArray = fileArray :+ subFs.getPath
        }
      }
      for (f <- fileArray) {
        log.info(f.toString)
      }
      fileArray
    } catch {
      case e: Exception =>
        log.error("com.pingan.pbear.NewsUtils.recursiveListFiles :  " + e.getMessage)
        log.info("recursiveListFiles  successful ......")
        fileArray
    }
  }

  /**
    * 获取文件系统
 *
    * @return FileSystem
    */
  def getFileSystem(): FileSystem = {
    log.info("BEGIN  getFileSystem ......")
    val conf: Configuration = new Configuration
    conf.addResource(new Path(configBasePath + "core-site.xml"))
    conf.addResource(new Path(configBasePath + "hdfs-site-tmp.xml"))
    conf.addResource(new Path(configBasePath + "yarn-site.xml"))
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    val fs: FileSystem = FileSystem.get(conf)
    log.info("getFileSystem  successful ......")
    fs
  }

  /**
    * 获取HDFS根目录
 *
    * @return 根目录路径
    */
  def getHdfsRoot(): String = {
    log.info("BEGIN getHdfsHome ......")
    val hdfsUsrHome: String = getFileSystem().getHomeDirectory.toString
    log.info("getHdfsHome successful ......")
    hdfsUsrHome.substring(0, hdfsUsrHome.indexOf("/", "hdfs://".size)) + "/"
  }

  /**
    * 删除文件或文件夹
 *
    * @param file
    */
  def deleteIfExist(file: String): Unit = {
    val fh = new Path(file)
    val fs = HDFSTool.getFileSystem()
    if (fs.exists(fh)) {
      fs.delete(fh, true)
    }
  }

}
