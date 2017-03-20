import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark
/**
  * Created by zhangrunqin on 17-1-3.
  * Es-hadoop 支持三种数据格式：Map,Case Class,Json
  */
object SparkToEsExample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkToEs")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "127.0.0.1")

    val sc = new SparkContext(conf)
    //case 1: Map -> ES
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

    //隐式调用.写入数据到ES
    //sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")

    //case 2: Case Class -> ES
    // define a case class
    case class Trip(departure: String, arrival: String)

    val upcomingTrip = Trip("OTP", "SFO")
    val lastWeekTrip = Trip("MUC", "OTP")

    val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
    //显示调用EsSpark写入数据到ES
    //EsSpark.saveToEs(rdd, "spark/docs")
    //EsSpark.saveToEs(rdd, "spark/docs", Map("es.mapping.id" -> "id"))

    //case 3: Json -> ES
    val json1 = """{"reason" : "business", "airport" : "SFO"}"""
    val json2 = """{"participants" : 5, "airport" : "OTP"}"""
    //sc.makeRDD(Seq(json1,json2)).saveJsonToEs("spark/json-trips")

    //dynamic/multi-resourcesedit
    /*
    //动态指定type
    val game = Map("media_type"->"game","title" -> "FF VI","year" -> "1994")
    val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
    val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")
    sc.makeRDD(Seq(game, book, cd)).saveToEs("my-collection/{media_type}")

    //自定义id
    EsSpark.saveToEs(rdd, "iteblog/docs", Map("es.mapping.id" -> "id"))

    //自定义元数据
    import org.elasticsearch.spark.rdd.Metadata._
    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")
    val otpMeta = Map(ID -> 1, TTL -> "3h") 
    val mucMeta = Map(ID -> 2, VERSION -> "23")
    val sfoMeta = Map(ID -> 3)

    sc.makeRDD(Seq((otpMeta, otp), (mucMeta, muc), (sfoMeta, sfo))).saveToEsWithMeta("iteblog/2015")
    */

//    val sqlCtx = new HiveContext(sc)
//    val df = sqlCtx.sql("select * from parquet.`hdfs://127.0.0.1:9000/bigdata/user_cube/part_0")
    val sqlCtx = new SQLContext(sc)
    //val df = sqlCtx.sql("select title,summary from json.`hdfs://127.0.0.1:9000/data/train/")
    val df = sqlCtx.read.json("hdfs://127.0.0.1:9000/data/train/").select("title", "createdTime")
    df.show()
    df.toJSON.foreach(println)
    df.toJSON.saveJsonToEs("spark/news1")
    val schema = df.schema
    val len = schema.fields.length
    //Map的类型固定成了String -> String
    val newDf = df.map(r => {
      var m = Map[String, String]()
      (0 until len).foreach(i => {
        m += (schema.fields(i).name -> r.getAs[String](i))
      })
      m
    })
    newDf.collect().take(20).foreach(println)
    //newDf.saveToEs("spark/news")
  }
}
