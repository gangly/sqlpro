import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import java.util.Properties
/**
  * Created by zhangrunqin on 16-12-26.
  */
object SparkSqlExample extends Logging{
  def main(args: Array[String]): Unit = {
    HiveToPg()
  }

  def HiveToPg():Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("HiveToPg").setMaster("local[2]"))
    val hiveCtx = new HiveContext(sc)
    val df = hiveCtx.sql("select * from parquet.`hdfs://127.0.0.1:9000/bigdata/user_cube/part_0`")
    df.show()
    val properties = new Properties()
    properties.put("user", "pbear")
    properties.put("password", "pbear888")
    df.write.mode("overwrite").jdbc("jdbc:postgresql://127.0.0.1:5432/pbeardb","user_table",properties)
    sc.stop()
  }
}
