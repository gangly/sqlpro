import java.util.{Properties, Random}

import com.typesafe.config.ConfigFactory
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import java.util.Properties
import java.util.Random

/**
  * Created by zhangrunqin on 16-11-22.
  */
object KafkaProducer {
  private val appCfg = ConfigFactory.load()

  def main(args: Array[String]) {
    val topics = Option(appCfg.getString("Kafka.topics")).getOrElse("news")
    val brokers = Option(appCfg.getString("Kafka.brokers")).getOrElse("localhost:9092")
    println(topics)
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val kafkaConfig = new ProducerConfig(props)
    val producer = new Producer[String,String](kafkaConfig)
    var counter = 0
    val r1 = new Random(97631)
    val r2 = new Random(79321)
    while (true){
      val id = r1.nextInt(5) + 1
      val uid = r2.nextInt(5) + 1
      val log = "[21/Nov/2016 10:49:31]\"GET /api/news?id=" + id.toString + "&uid=" + uid.toString + " HTTP/1.1\" 200 1767"
      producer.send(new KeyedMessage[String,String](topics,log))
      println(log)
      counter += 1
      println(counter)
      Thread.sleep(2000)
    }
  }
}
