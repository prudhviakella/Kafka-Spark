package spark.streaming.structured.j_StreamJoins

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/*
*Author: Prudhvi Akella.
* Desc: This App is used to join the Batch and Streaming Data
 */
/*Spark Streaming allows to perform join operations between two streams or between batch data and streaming data
* Lets see and Example of join with batch and streaming*/
//Batch Data : For batch data refer to datasets\customerDatasets\static_datasets
//batch data consists of customer specific info lets assume we have a store  for every customer we have a
// customer_id(unique id for every customer),Sex,Age
//Streaming Data : datasets\customerDatasets\streaming_datasets\join_streaming_transaction_details
//Streaming data contains the transactions information for every customer.for every transaction that the customer make
//we have customer_ID,Transaction Amount, Transaction Rating
object a_Join_Batch_Streaming extends App {
  System.setProperty("hadoop.home.dir", "D:\\spark")
  val spark = SparkSession
    .builder
    .appName("JoinBatchStream")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR");
  /*Lets Create a schema.StructTypes contains list of StructField object that define name,type,nullable*/
  /*Every field in the dataset is nullable that means the info might be missing in our dataset */
  val personal_details_schema = StructType(
    List(
      StructField("Customer_ID", StringType, nullable = true),
      StructField("Gender", StringType, nullable = true),
      StructField("Age", StringType, nullable = true)
    )
  )
  val customerDF = spark.read
    .format("csv")
    .option("header","true")
    .schema(personal_details_schema)
    .load("D:\\spark\\apache-spark-2-structured-streaming\\02\\demos\\datasets\\customerDatasets\\static_datasets\\join_static_personal_details.csv")
  val transactions_details_schema = StructType(
    List(
      StructField("Customer_ID", StringType, nullable = true),
      StructField("Transaction_Amount", StringType, nullable = true),
      StructField("Transaction_Rating", StringType, nullable = true)
    )
  )
  /*Every file of customer transactions new batch will be triggered*/
  val TranscationStream = spark
    .readStream.option("header","true")
    .option("maxFilesPerTrigger",1)
    .schema(transactions_details_schema)
    .csv("D:\\spark\\apache-spark-2-structured-streaming\\02\\demos\\datasets\\customerDatasets\\streaming_datasets\\join_streaming_transaction_details")

  val joinedDF = customerDF.join(TranscationStream,Seq("Customer_ID"))
  //Seq("Customer_ID") any idea why did we did this to avoid ambiguity  issue.
  //Try with just with $"Customer_ID"
  val query = joinedDF.writeStream
    .outputMode("append")
    .format("console")
    .start()
    .awaitTermination()
}
