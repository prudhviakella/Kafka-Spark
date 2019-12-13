package spark.streaming.structured.i_TwitterStreaming

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split, udf,window}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/*
* Author : Prudhvi Akella.
* Description : This Application connects to the socket that is exposed by TwitterStreaming application and
* process the twitter data that is captured by twitter4j streaming library
*/
object b_SparkTwitterSocketStream extends App {
  if(args.length != 2) {
    println("Either host name or port is missing")
    System.exit(1)
  }
  val host:String=args(0).toString;
  val port:String=args(1).toString;
  println(host+" "+port)
  System.setProperty("hadoop.home.dir", "D:\\spark")
  //Creating spark session object to create socket stream
  val spark = SparkSession.builder
    .appName("SparkTwitterSocketStreaming")
    .master("local")
    .getOrCreate();
  spark.sparkContext.setLogLevel("ERROR")
  //Creating socket stream
  val lines = spark.readStream
    .format("socket")
    .option("host",host)
    .option("port",port)
    .option("includeTimestamp",true)
    .load()

  lines.printSchema()
  import spark.implicits._
  val words = lines.select(explode(split($"value"," ")).as("word"),$"timestamp")
  val extract_tags_udf = udf((word:String)=>{
    if(word.toLowerCase.startsWith("@")){
      word
    } else{
      "nonTag"
   }
  },StringType)
  val resultDF = words.withColumn("tags",extract_tags_udf($"word"))
  val windowHashTagCounts = resultDF
    .where($"tags".notEqual("nonTag"))
    .groupBy(window($"timestamp","50 seconds","30 seconds"),$"tags")
    .count()
    .orderBy($"count".desc)

  val query = windowHashTagCounts.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate",false)
    .start()

  query.awaitTermination()
}
