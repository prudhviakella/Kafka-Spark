package spark.streaming.structured.j_StreamJoins.Window

/*
*Author : Prudhvi Akella.
* Desc : If you want to extract insights about how you your customers spend you want the average transaction amount
* information of your customers split by age group but a certain intervals of time.which means you need to use
* windows
 */
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udf,window,avg}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Join_Window extends App {
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
  //Adding Timestamp with help us in performing windows operations in our later sessions.
  val AddtimestampUDF = udf(()=>{
    val now = Calendar.getInstance().getTime()
    val date = new SimpleDateFormat("Y-m-d H:M:s")
    date.format(now)
  })
  val FileStreamWithTS = joinedDF.withColumn("timestamp",AddtimestampUDF())
  /*We need to know the Average customer spends within a window and we want it on per age group basis*/
  val add_age_group_udf = udf((age:Int)=>{
    if(age < 25) "less Than 25"
    else if(age < 40) "25 to 40"
    else "more than 40"
  },StringType)
  val ratings_with_agegroup = FileStreamWithTS.withColumn("Age_group",add_age_group_udf($"Age"))
  /*The group your data into window intervals*/
  var widowed_transcations = ratings_with_agegroup.groupBy(window($"timestamp","10 seconds","5 seconds")
    ,$"Age_group").agg(avg($"Transaction_Amount".cast("Double")))
  /*Round of this amount to 2decimal places*/
  val round_udf = udf((amount:Double)=>{
    f"$amount%.2f"
  },StringType)

  widowed_transcations = widowed_transcations.withColumn("Average_Transcation_Amount",
    round_udf($"avg(CAST(Transaction_Amount AS DOUBLE))"))
    .drop($"avg(CAST(Transaction_Amount AS DOUBLE))")

  val query = widowed_transcations.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate",true)
    .start()
    .awaitTermination()
  /*Notice that Every time window have the various age groups and average amount spend by each age group
  * Its possible that this data can give us interesting insights about spending patterns of an individual age
  * groups at different time intervals  */
}
