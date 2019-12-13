package spark.streaming.structured.j_StreamJoins

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object c_JoinAggregations_Rating extends App {
  /*
*Author: Prudhvi Akella.
* Desc: This App is used to join the Batch and Streaming Data and performs Aggregations to
* Aggregate Rating by Ages
 */
  /*Spark Streaming allows to perform join operations between two streams or between batch data and streaming data
  * Lets see and Example of join with batch and streaming*/
  //Batch Data : For batch data refer to datasets\customerDatasets\static_datasets
  //batch data consists of customer specific info lets assume we have a store  for every customer we have a
  // customer_id(unique id for every customer),Sex,Age
  //Streaming Data : datasets\customerDatasets\streaming_datasets\join_streaming_transaction_details
  //Streaming data contains the transactions information for every customer.for every transaction that the customer make
  //we have customer_ID,Transaction Amount, Transaction Rating
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
  import spark.implicits._
  val joinedDF = customerDF.join(TranscationStream,Seq("Customer_ID")).withColumn("Age",$"Age".cast("Int"))

  val add_age_group_udf = udf((age:Int)=>{
    if(age < 25) "less Than 25"
    else if(age < 40) "25 to 40"
    else "more than 40"
  },StringType)

  val ratings_with_agegroup = joinedDF.withColumn("Age_group",add_age_group_udf($"Age"))

  val ratings_peragegroup = ratings_with_agegroup.groupBy("Age_group").agg(avg($"Transaction_Amount"))

  val query = ratings_peragegroup.writeStream
    .outputMode("complete")
    .format("console")
    .start()
    .awaitTermination()

}
