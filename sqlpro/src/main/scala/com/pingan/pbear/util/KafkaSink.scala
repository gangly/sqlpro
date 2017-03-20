package com.pingan.pbear.util

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

/**
  * Created by lovelife on 17/2/20.
  */
class KafkaSink(createProducer: () => Producer[String, String]) extends Serializable {

  lazy val producer = createProducer()
  def send(topics: String, message: String) = producer.send(new KeyedMessage[String, String](topics, message))
  def send(topics: String, key: String, message: String) =  producer.send(new KeyedMessage[String, String](topics, key, message))
}

object KafkaSink {
  def apply(props: Properties): KafkaSink = {
    val f = () => {
      val producer = new Producer[String, String](new ProducerConfig(props))
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new KafkaSink(f)
  }

  def apply(brokers: String): KafkaSink = {
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    apply(props)
  }
}