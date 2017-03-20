package com.pingan.pbear.test

import com.pingan.pbear.udf.common.GeoIp
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lovelife on 16/12/27.
  */
object TestSparkSql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test sparksql").setMaster("local[2]") // to remove
    val sc = new SparkContext(conf)
//    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
//
//    sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
//    sqlContext.sql("LOAD DATA LOCAL INPATH '/Users/lovelife/program/spark/sparksql/example/kv.txt' INTO TABLE src")
//
//    // Queries are expressed in HiveQL
//    sqlContext.sql("FROM src SELECT key, value").collect().foreach(println)

//    /****json exam***/
//
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//
//    // Create the DataFrame
//    val df = sqlContext.read.json("/Users/lovelife/program/spark/sparksql/example/people.json")
//
//    // Show the content of the DataFrame
//    df.show()

//    /**** jdbc exam ****/
//    val sqlContext = new SQLContext(sc)
//    val jdbcDF = sqlContext.read.format("jdbc").options(
//      Map("url" -> "jdbc:postgresql://localhost:5432/test",
//        "dbtable" -> "stu", "driver" -> "org.postgresql.Driver",
//      "user" -> "postgres",
//      "password" -> "1234@abcd")).load()
//    jdbcDF.show()

    /**** Geoip udf test ****/
//    CREATE TEMPORARY FUNCTION parser AS 'com.pingan.pbear.udfs.LineParser')
    val hiveCtx = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveCtx.udf.register("ip2city", GeoIp.ip2city _)
    val ret = hiveCtx.sql("select ip, ip2city(ip) from test.geoip")
    ret.map(t => t(0) + "|" + t(1)).collect().foreach(println)

  }
}
