package kafka.publish_subcribe

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import scala.collection.JavaConverters._

/*
*Note: If you want to run the same program twice or in parallel in intelliJ
* go to Run->Run configuration-> select program -> check allow run parallel -> click apply.
 */
/*
*Note:For experiencing the consumer groups have to run the program two times so that
* the consumer group rebalancing  will happen so that consumer will share the partitions
* across the group . But doing this you have run kafka at-least with two instance.
*/
object d_SimpleConsumer extends App {

  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  //This property is very important.Once assgined don't change it because offsets will be commited using this.
  //If you change this property consumer will start reading the messages from every first.
  //Other use of it is it says this consumer belongs to which cosumer group
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka_consumer2")
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
  props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
  /**
   * The timeout used to detect worker failures. The worker sends periodic heartbeats to indicate
   * its liveness to the broker.
   */
  props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
  //earliest : read from very beginning,latest means read most recent ones, none: nothing
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer = new KafkaConsumer[String, String](props)
  //singletonList returns the immutable list of objects.
  consumer.subscribe(Collections.singletonList("callback-Producer"))
  while (true) {
    val record = consumer.poll(1000).asScala
    for (data <- record)
      println("Key:"+data.key()+"Value:"+data.value())
  }
}
