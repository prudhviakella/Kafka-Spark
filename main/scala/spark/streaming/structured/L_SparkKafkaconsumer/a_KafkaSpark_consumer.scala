package spark.streaming.structured.L_SparkKafkaconsumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType,TimestampType,ArrayType}

object a_KafkaSpark_consumer {
  def main(args:Array[String]) {
    val schema = StructType(
      List(
        StructField("timestamp", StringType, nullable = true), //every crime has a code associated with it
        StructField("system", StringType, nullable = true), //a town with a corporation
        StructField("actor", StringType, nullable = true), //whether its a theft or domestic altercation
        StructField("action", StringType, nullable = true), //whether its a theft or domestic altercation
        StructField("objects", ArrayType(StringType, true)), //the number of convection which resulted for crime report.
        StructField("location", StringType, nullable = true), //Year and month when crime occured.
        StructField("message", StringType, nullable = true)
      )
    )
    val spark = SparkSession
      .builder
      .appName("sparkKafka2")
      .master("local")
      .getOrCreate()

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "jackieChanCommand")
      .option("startingOffsets", "earliest")
      .load()

    spark.sparkContext.setLogLevel("ERROR");

    import spark.implicits._

    val ds = df.select($"value" cast "string" as "json")
      .select(from_json($"json", schema) as "data")
      .select("data.*")

    ds.printSchema()

    //ds.select($"action")

    ds.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }
}
