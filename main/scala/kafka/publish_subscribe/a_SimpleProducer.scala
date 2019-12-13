package kafka.publish_subcribe

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object a_SimpleProducer  {
  def main(args:Array[String]): Unit ={
    println("Started Processing..")
    //Defining the kafka producer Properties
    val props = new Properties()
    //Producer should know where is brokers running
    props.put("bootstrap.servers", "localhost:9092")
    //Key Serializer
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //Value Serializer
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    for(i <- 1 to 100){
      val record = new ProducerRecord[String, String]("Second-Producer",i.toString , "Message"+i)
      producer.send(record)
    }
    producer.flush()
    producer.close()
    println("Completed Processing..")
  }
}
