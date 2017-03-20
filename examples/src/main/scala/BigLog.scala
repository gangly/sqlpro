import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.JdbcRDD
import java.sql.DriverManager
/**
  * Created by zhangrunqin on 16-12-2.
  */
object BigLog {
  def main(args: Array[String]) {
    println("SQL Based server test")
    val sc = new SparkContext(new SparkConf().setAppName("BigLog").setMaster("local[2]"))
    val rdd = new JdbcRDD(sc, ()=>{
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata","root","zrq123456")
    },"select id,url from wallstreet where 1 = ? and 2 = ? and url like 'http://wallstreetcn.com/node/2722%' and id in(select id from crawled_url)", 1, 2, 1,r => (r.getInt(1),r.getString(2)))
    rdd.foreach(r => println(r._1, r._2))
  }

  /**
    * 根据配置文件解析SQL语句
    * @param configName
    */
  def parser(configName: String): Unit ={

  }

  /**
    * 根据SQL语句生成执行任务
    */
  def genTask(): Unit ={

  }
}
