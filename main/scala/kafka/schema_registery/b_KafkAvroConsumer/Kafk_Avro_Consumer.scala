package kafka.schema_registery.b_KafkAvroConsumer

import java.io.ByteArrayInputStream
import java.util.{Collections, Properties}

import com.sksamuel.avro4s.AvroInputStream
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConverters._

final case class Product(color: Option[String], productType: Option[String] , designType : Option[String])
final case class UserId(userId : Option[String])

object Kafk_Avro_Consumer {
  def main(args:Array[String]): Unit ={
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    //This property is very important.Once assgined don't change it because offsets will be commited using this.
    //If you change this property consumer will start reading the messages from every first.
    //Other use of it is it says this consumer belongs to which cosumer group
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka_consumer6")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    /**
     * The timeout used to detect worker failures. The worker sends periodic heartbeats to indicate
     * its liveness to the broker.
     */
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    //earliest : read from very beginning,latest means read most recent ones, none: nothing
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getCanonicalName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,classOf[KafkaAvroDeserializer].getCanonicalName)
    props.put("schema.registry.url", "http://localhost:8081")

    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
    //singletonList returns the immutable list of objects.
    consumer.subscribe(Collections.singletonList("test1"))
    while (true) {
      val record = consumer.poll(1000).asScala
      for (data <- record) {
        println("-----------------------------------------------------------------------")
        println("Before Conversion:->"+data.key()+":"+data.value())
        println("After Conversion:->"+"Key:"+returnKey(data.key()).toString+"Value:"+returnValue(data.value()).toString)
        val key:UserId = returnKey(data.key())
        val value:Product = returnValue(data.value())
        println(key.userId.getOrElse("").toString+"->"+value.color.getOrElse("").toString+":"+value.designType.getOrElse("").toString+":"+value.productType.getOrElse("").toString)
        println("-----------------------------------------------------------------------")
      }
      //
    }
  }

  def returnKey(key:Array[Byte]):UserId={
    val in = new ByteArrayInputStream(key)
    val input = AvroInputStream.binary[UserId](in)
    input.iterator.toSeq.head
  }
  def returnValue(value:Array[Byte]):Product={
    val in = new ByteArrayInputStream(value)
    val input = AvroInputStream.binary[Product](in)
    input.iterator.toSeq.head
  }
}
