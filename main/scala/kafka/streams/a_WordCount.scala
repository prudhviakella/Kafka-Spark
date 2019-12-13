package kafka.streams


import java.lang
import java.time.Duration
import java.util.Properties

import org.apache.log4j.Logger
import org.apache.kafka.streams.scala.{ByteArrayWindowStore, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.Materialized._

/*
* Compiler : Scala 2.11.8 with -xExperimental Flag enabled.
* Producer console command : kafka-console-producer.bat --broker-list localhost:9092 --topic word-count-input
* Consumer console command : kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic word-count-output --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
* Logging:log4j Note: Its important to have log4j.properties file in main/resources folder or else you don't see much logging info
* Note: Streams acts like both consumer and producer it has both the properties in it and you can clearly observer them in
* logging info(ConsumerConfig,ProducerConfig)
 */
object a_WordCount extends App{
  val logger = Logger.getLogger(this.getClass.getName)
  //Preparing the properties that are required for kafka stream
  //APPLICATION_ID plays an important role streams engine uses it will  like consumer-id,default-clinet-id prefix,
  // prefix-to internal logs.Once stream is created don't change if you change it stream engine consider it at as new
  //stream and it will start from beginning
  val config: Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")//consumer property
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    properties
  }

  val builder = new StreamsBuilder()
  // 1 - stream from Kafka (Source Processor)
  val textLines: KStream[String, String] = builder.stream[String, String]("word-count-input")

  val wordCounts:KTable[String,Long] = textLines
  // 2 - map values to lowercase
    .mapValues(Value => Value.toLowerCase)
  // 3 - flatmap values split by space
    .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
  // 4 - select key to apply a key (As key is null we applying key. As of now key is nothing but value)
    .selectKey((key,value)=>value)
  // 5 - group by key before aggregation
    .groupByKey
  // 6 - count occurences
    .count()

  /*
  *Creating a new branches based on the predicates or conditions
  * we can do this using branch function which returns Array of branches
  * its more like Nested if else
  * in the below example it creates two branches
  * if value > 100 it goes into one branch that is branch(0)
  * if value > 10 it goes into one branch that is branch(1)
  * if value > 2 it goes into one branch that is branch(2)
  * else records will be dropped.
   */
  val branches = wordCounts
    .toStream
    .branch((key,value)=>value>100,
    (key,value)=>value>10,
    (key,value)=>value>2)

  //word-count-output to kafka topic (Sink Processor)
  wordCounts.toStream.to("word-count-output")
  //builder.build() returns a topology and KafkaStreams creates a stream with Topology and config
  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
  streams.cleanUp()
  streams.start()
  println("Topology:")
  //print the topology
  streams.localThreadsMetadata().forEach(t => println(t.toString))
  // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      streams.close()
    }
  })
}
