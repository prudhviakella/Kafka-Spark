package kafka.publish_subcribe

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

object c_Callback_Producer extends App{
  //Defining the kafka producer Properties
  val props = new Properties()
  //Producer should know where is brokers running
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  //Key Serializer
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  //Value Serializer
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  //Creating Idempotence Producer
  props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
  props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
  props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  //Available types are lz4,snappy,gzip.
  props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
  props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024))
  props.put(ProducerConfig.LINGER_MS_CONFIG, "20")
  //There is a property call min.insync.replica can be set in a broker or topic level
  //if the property is set to 2 then two brokers including leader has to respond that they
  //have the data otherwise you get the error
  //That means lets say replication.factor=3 , insync.replica =2 and akws=all then we can only tolorate only broker going down.
  //otherwise producer will receive the exception(NOT_ENOUGH_REPLICAS).
  val producer = new KafkaProducer[String, String](props)
  for(i <- 1 to 100){
    val record = new ProducerRecord[String, String]("callback-Producer",i.toString , "Message"+i)
    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if(exception == null){
            println("Topic_name:"+metadata.topic()+":"+"Partition:"+metadata.partition()
            +":offset:"+metadata.offset()+":"+"Timestamp:"+metadata.timestamp())
          }
          else println("Error while sending the data:" + exception.getCause)
      }
    })
  }
  producer.flush()
  producer.close()
}
