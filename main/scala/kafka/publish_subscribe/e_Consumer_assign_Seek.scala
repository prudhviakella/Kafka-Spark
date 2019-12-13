package kafka.publish_subcribe

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

object e_Consumer_assign_Seek extends App{
  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  //This property is very important.Once assgined don't change it because offsets will be commited using this.
  //If you change this property consumer will start reading the messages from every first.
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka_consumer2")
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
  props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
  props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
  //earliest : read from very beginning,latest means read most recent ones, none: nothing
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer = new KafkaConsumer[String, String](props)
  //assign and seek are mostly used to replay data or to fetch the specific messages.
  // from callback-Producer am getting 0th partition
  val partition = new TopicPartition("callback-Producer", 0)
  consumer.assign(Collections.singletonList(partition))
  consumer.seek(partition, 15)
 // consumer.subscribe(Collections.singletonList("callback-Producer"))
  while (true) {
    val record = consumer.poll(1000).asScala
    for (data <- record)
      println("Key:"+data.key()+"Value:"+data.value())
  }
}
