/*
Author : Prudhvi Akella
Name: AppendMode
Description : Append Mode with files Example
*/
package spark.streaming.structured.b_AppendMode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/*Spark has an ability to treat files added to a directory as streaming data.
* This will be extremely useful if you have process like Hadoop MR jobs which output files into a particular directory
* at periodic intervals   */
/*We are going to work on directory which has a files about london crime.Each file contains few thousand lines crime reports
* from the city of london.So lets create one droplocation directory to which our spark streaming will be listening to.*/
object a_AppendMode {
  def main(args:Array[String]): Unit ={
    //System.setProperty("hadoop.home.dir", "D:\\spark")
    val spark = SparkSession
      .builder
      .appName("StructuredFileStream")
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
      .schema(schema)
      .csv("/home/cloudera/spark/datasets/droplocation")

    println("Is the stream ready?")
    println(Filedataset.isStreaming)//isStreaming indicates that its streaming data.
    println("Schema of Input Stream:")
    Filedataset.printSchema()
    import spark.implicits._
    val trimmedDS = Filedataset.select("borough","year","month","value")
      .withColumnRenamed("value","convictions")

    val query = trimmedDS
      .writeStream
      .outputMode("append")//only new rows appended to the result since the last trigger will be written to sink(console)
      .format("console")
      .option("truncate","false")//Whether to truncate the output if it is too long.default true
      .option("numRows",30)//At Every trigger we print out 30 rows to screen.the default trigger is set to 0 seconds which
      // means each time previous output is processed and new stream data comes in that is processed immediately.
      .start()
    //It waits until program terminates
    query.awaitTermination()

  }
}
