package spark.streaming.structured.g_GroupTimestamp

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum, udf}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import spark.streaming.structured.d_Aggregations.a_Aggregations.{Filedataset, convictionsPerBorough}

object GroupDataTimestamp extends App {
  System.setProperty("hadoop.home.dir", "D:\\spark")
  val spark = SparkSession
    .builder
    .appName("StructuredStreamingAggregations")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR");
  /*Lets Create a schema.StructTypes contains list of StructField object that define name,type,nullable*/
  /*Every field in the dataset is nullable that means the info might be missing in our dataset */
  val schema = StructType(
    List(
      StructField("lsoa_code", StringType, nullable = true),//every crime has a code associated with it
      StructField("borough", StringType, nullable = true),//a town with a corporation
      StructField("major_category", StringType, nullable = true),//whether its a theft or domestic altercation
      StructField("minor_category", StringType, nullable = true),//whether its a theft or domestic altercation
      StructField("value", StringType, nullable = true),//the number of convection which resulted for crime report.
      StructField("year", StringType, nullable = true),//Year and month when crime occured.
      StructField("month", StringType, nullable = true)
    )
  )
  /*Lets instantiate file stream data*/
  val Filedataset = spark
    .readStream
    .option("header","true")
    .option("maxFilesPerTrigger",2)//The number of Files read in by the File stream source at a time-serves as a
    // kind of rating limit setting it to two means spark will trigger a re-computation of the results table when it reads in
    //two files from the file streaming
    //Lets say if already 4 files are already exist in directory how many batches will be created?
    .schema(schema)
    .csv("D:\\spark\\apache-spark-2-structured-streaming\\02\\demos\\datasets\\droplocation")
  /*Registering UDF with sparkSession*/
  // spark.udf.register("add_timestamp",add_timestamp())
  //Adding Timestamp with help us in performing windows operations in our later sessions.
  val AddtimestampUDF = udf(()=>{
    val now = Calendar.getInstance().getTime()
    val date = new SimpleDateFormat("Y-m-d H:M:s")
    date.format(now)
  })
  val FileStreamWithTS = Filedataset.withColumn("timestamp",AddtimestampUDF())
  import spark.implicits._
  val convictionsbasedonTS = FileStreamWithTS
    .groupBy("borough").agg(sum($"value".cast("Int")))
    //.agg(Map("value"->"sum"))
    .withColumnRenamed("sum(CAST(value AS INT))","convictions")
    .orderBy($"convictions".desc)
  val query = convictionsbasedonTS
    .writeStream
    .outputMode("complete")//It recomputes all the rows in the result table and writes it out or
    //entire result table will be recomputed
    .format("console")
    .option("truncate","false")//Whether to truncate the output if it is too long.default true
    .trigger(Trigger.ProcessingTime("5 seconds"))//This trigger says how often this query runs.this is once every 5 seconds
    .option("numRows",30)//At Every trigger we print out 30 rows to screen.the default trigger is set to 0 seconds which
    // means each time previous output is processed and new stream data comes in that is processed immediately.
    .start()
  //It waits until program terminates
  query.awaitTermination()
}
