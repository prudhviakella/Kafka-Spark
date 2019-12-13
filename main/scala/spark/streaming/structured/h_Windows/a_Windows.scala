package spark.streaming.structured.h_Windows

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum, udf, window}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/*For Understanding it more better we should have at least 4 files in drop locations*/
object a_Windows extends App{
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
    val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    date.format(now)
  })
  val FileStreamWithTS = Filedataset.withColumn("timestamp",AddtimestampUDF())
  import spark.implicits._
  //Find the total number of convictions for a particular time interval. You can imagine we are receiving these crime reports
  //in real time and we want to see how many of these resulted in a convictions for every interval.We can groupBy on window
  //to apply aggregations to all records which fall within this is sliding interval
  //Window function takes there parameters in this is case.
  //1)TimeColumn: on which column you wanted to perform windowing
  //2)windowDuration: fixed Interval
  //3) Slide Duration : slide Interval
  //If you mention slide Duration it is performing sliding window operations if you don't it performs thumbing operations.
  val WindowCounts = FileStreamWithTS
    .groupBy(
      window($"timestamp","10 seconds","5 seconds")
    ).agg(sum($"value".cast("Int")))
    //.agg(Map("value"->"sum"))
    .withColumnRenamed("sum(CAST(value AS INT))","convictions")
    .orderBy($"convictions".desc)
  val query = Filedataset
    .writeStream
    .outputMode("append")//It recomputes all the rows in the result table and writes it out or
    //entire result table will be recomputed
    .format("console")
    .option("numRows",30)//At Every trigger we print out 30 rows to screen.the default trigger is set to 0 seconds which
    .option("truncate","false")//Whether to truncate the output if it is too long.default true
    // means each time previous output is processed and new stream data comes in that is processed immediately.
    .start()
  //It waits until program terminates
  query.awaitTermination()


}
