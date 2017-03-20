package com.pingan.pbear.udf.nlp

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import com.pingan.pbear.util.HDFSTool
import java.io.{BufferedInputStream, BufferedReader, InputStreamReader}
import com.pingan.pbear.common.AppConf
import org.ansj.recognition.impl.FilterRecognition
import org.ansj.library.UserDefineLibrary
import org.ansj.splitWord.analysis.NlpAnalysis
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.nlpcn.commons.lang.util.StringUtil

/**
  * Created by zhangrunqin on 16-12-1.
  * 分词服务接口,需要在/conf/nlp.conf中添加配置文件,配置停用词/用户自定义词典位置等
  * [非Spark环境,不会设计spark上下文,以支持普通的java程序调用]
  */
object AnsjNLP {

  //设置停用词
  private val filter: FilterRecognition = new FilterRecognition()
  filter.insertStopNatures("c")  //连词
  filter.insertStopNatures("uj")  //助词
  filter.insertStopNatures("ul")  //助词
  filter.insertStopNatures("p")  //介词
  filter.insertStopNatures("e")  //叹词
  filter.insertStopNatures("y")  //语气词
  filter.insertStopNatures("o")  //拟声词
  filter.insertStopNatures("w")  //标点符号
  filter.insertStopNatures("q")  //量词
  filter.insertStopNatures("null")  //其他
  initAnsjNLP()

  def initAnsjNLP() = {
    val fs = HDFSTool.getFileSystem()
    val hdfsRoot = AppConf.getHdfsRoot
    loadStopWords(fs, hdfsRoot + AppConf.getStopWordFile)
    loadUserdefineWords(fs, hdfsRoot + AppConf.getUserDictFile)
  }

  /**
    * 加载停用词词典
    *
    * @param fs
    * @param file
    */
  def loadStopWords(fs: FileSystem, file: String): Unit = {
    println(s"load stopword file: $file")
    try {
      val fds = new FSDataInputStream(fs.open(new Path(file)))
      val br = new BufferedReader(new InputStreamReader(fds))
      var line = StringUtil.trim(br.readLine())
      while (line != null && line.length > 0) {
        filter.insertStopWord(line)
        line = StringUtil.trim(br.readLine())
      }
    }catch {
      case e: Exception => e.printStackTrace()
    }
  }


  /**
    * 从HDFS加载用户字典
    *
    * @param fs
    * @param dictPath 字典路径,会读取所有以.dic结尾的文件进行解析
    * @return
    */
  def loadUserdefineWords(fs: FileSystem, dictPath: String): Unit = {
    try {
      val res = new ArrayBuffer[(String, String, Int)]()
      val fsStatus = fs.getFileStatus(new Path(dictPath))
      val files = HDFSTool.recursiveListFiles(fsStatus, fs)
      files.filter(_.getName.endsWith(".dic")).foreach(f => {
        val file = fs.open(f)
        val is = new BufferedInputStream(file, 12 * 1024)
//        var tmp = 0
        val strBuild: StringBuilder = new StringBuilder()
        val byteArr = new Array[Byte](12 * 1024)
//        while ((is.available() > 0) && (tmp = is.read(byteArr)) != -1) {
//          strBuild.append(new String(byteArr, 0, tmp))
//        }
        if (is.available() > 0) {
          var tmp = is.read(byteArr)
          while (tmp != -1) {
            strBuild.append(new String(byteArr, 0, tmp))
            tmp = is.read(byteArr)
          }
        }
        file.close()
        is.close()
        strBuild.toString.split("\n").foreach(line => {
          if (line.trim.nonEmpty) {
            val columns = line.split("\t")
            if (columns.length == 3) {
              res += ((columns(0), columns(1), columns(2).toInt))
            }
          }
        })
        //println(strBuild.toString())
        strBuild.clear()
      })
      res.foreach(w => {
        UserDefineLibrary.insertWord(w._1, w._2, w._3)
      })
    }catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def ansjWordSeg(text: String): Seq[String] = {
    val textNormal = text.replaceAll("\t|\n|\r", "")
    val result = NlpAnalysis.parse(textNormal).recognition(filter) //过滤分词结果,filter不可串行化,要分别赋值
    //result.foreach(r => println(s"${r.getName}\t${r.getNatureStr}"))
    result.map(term => term.getName).toSeq
  }


  def ansjWordSegToString(text: String): String = {
    ansjWordSeg(text).mkString("|")
  }




  def main(args: Array[String]): Unit = {
    println("AnsjNLP split test")
    val fs = HDFSTool.getFileSystem()
    val hdfsRoot = AppConf.getHdfsRoot
    loadStopWords(fs, hdfsRoot + AppConf.getStopWordFile)
    loadUserdefineWords(fs, hdfsRoot + AppConf.getUserDictFile)
    val doc1 = "500彩票网,据世卫组织统计，当今超过1400万艾滋病毒感染者仍然对自身状况一无所知，其中的许多人是艾滋病毒感染的高风险人群，他们往往很难获得现有检测服务,澳大利亚失业率消费者情绪指数,"
    ansjWordSeg(doc1).foreach(println)
  }
}

