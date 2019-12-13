package kafka.schema_registery.b_KafkAvroProducer

import java.util.Properties

import com.sksamuel.avro4s.{AvroOutputStream, AvroSchema}
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalacheck.Gen

final case class Product(color: Option[String], productType: Option[String] , designType : Option[String])
object Product {
  val product: Gen[Product] = for {
    color <- Gen.option(Gen.oneOf("GREEN", "BLUE", "PURPLE"))
    productType <- Gen.option(Gen.oneOf("TSHIRT", "DESIGN"))
    designType  <- Gen.option(Gen.oneOf("NONE", "SUITCASE", "CAR","WARNING"))
  } yield Product(color, productType, designType)
}

final case class UserId(userId : Option[String])
object UserId{
  val userid: Gen[UserId] = for{
    userId <- Gen.option(Gen.oneOf("ABC123", "ABC321", "CBA123", "CBA321", "A1B2C3"))
  }yield UserId(userId)
}

class EventGenerator{
  val productevent: Product = Product.product.sample.get
  val userEvent: UserId = UserId.userid.sample.get
}

object KafkaProducer {
  def main(args:Array[String]): Unit ={
    //Defining the kafka producer Properties
    val props = new Properties()
    //Producer should know where is brokers running
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    //Key Serializer
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getCanonicalName)
    //Value Serializer
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getCanonicalName)
    //Creating Idempotence Producer
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    //Available types are lz4,snappy,gzip.
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024))
    props.put(ProducerConfig.LINGER_MS_CONFIG, "20")
    props.put("schema.registry.url", "http://localhost:8081")
    //There is a property call min.insync.replica can be set in a broker or topic level
    //if the property is set to 2 then two brokers including leader has to respond that they
    //have the data otherwise you get the error
    //That means lets say replication.factor=3 , insync.replica =2 and akws=all then we can only tolorate only broker going down.
    //otherwise producer will receive the exception(NOT_ENOUGH_REPLICAS).
//    val key_schema=AvroSchema[UserId]
//    val Value_schema=AvroSchema[Product]
//    println(key_schema)
//    println(Value_schema)
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)
    for(i <- 1 to 10){
     val event = new EventGenerator()
      val key = event.userEvent
      val value = event.productevent
      val KeyBytes: Array[Byte] = {
        val baos = new ByteArrayOutputStream
        val output = AvroOutputStream.binary[UserId](baos)
        output.write(key)
        output.close()
        baos.toByteArray
      }
      val ValueBytes: Array[Byte] = {
        val baos = new ByteArrayOutputStream
        val output = AvroOutputStream.binary[Product](baos)
        output.write(value)
        output.close()
        baos.toByteArray
      }
      val record = new ProducerRecord("test1",KeyBytes, ValueBytes)
      producer.send(record)
    }
    producer.flush()
    producer.close()
  }
}
