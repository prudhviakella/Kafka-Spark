package spark.streaming.structured.e_SqlQuieres

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import spark.streaming.structured.d_Aggregations.a_Aggregations.{Filedataset, convictionsPerBorough, spark}

object SqlQuieres extends App {
  /*Spark has an ability to treat files added to a directory as streaming data.
  * This will be extremely useful if you have process like Hadoop MR jobs which output files into a particular directory
  * at periodic intervals   */
  /*We are going to work on directory which has a files about london crime.Each file contains few thousand lines crime reports
  * from the city of london.So lets create one droplocation directory to which our spark streaming will be listening to.*/
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
  //In order to run sql quires on streaming data we need to register dataframe as temporary view
  Filedataset.createOrReplaceTempView("LondonCrimedata");
  val CategoryDf = spark.sql("SELECT major_category,value " +
                                      "FROM LondonCrimedata " +
                                      "WHERE YEAR = '2016'")
  import spark.implicits._
  val convictionsPerBorough = CategoryDf
    .groupBy("major_category")
    .agg(sum($"value".cast("Int"))) //If i don't cast to Int will it work?
    //.agg(Map("value"->"sum"))
    .withColumnRenamed("sum(CAST(value AS INT))","convictions")
    .orderBy($"convictions".desc)

  val query = convictionsPerBorough
    .writeStream
    .outputMode("complete")//It recomputes all the rows in the result table and writes it out or
    //entire result table will be recomputed
    .format("console")
    .option("truncate","false")//Whether to truncate the output if it is too long.default true
    .option("numRows",30)//At Every trigger we print out 30 rows to screen.the default trigger is set to 0 seconds which
    // means each time previous output is processed and new stream data comes in that is processed immediately.
    .start()
  //It waits until program terminates
  query.awaitTermination()
}
