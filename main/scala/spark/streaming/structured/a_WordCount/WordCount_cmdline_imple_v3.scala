package spark.streaming.structured.a_WordCount

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}

/*
* for making a jar follow below instructions
* Click on file->Project Structure-> Artifacts->click on + symbol->select JAR --> Click on from modules with dependency->select class that you want to jar
* -->click ok-->click apply-->click ok
* Now once that is done go to build-->click on build Artifact--> build
* Its takes a while to generate a jar wait.. Once done you can see jar in out/artifacts/<projectname>/\*.jar
 */
/*Spark Setup Intellij
* https://kaizen.itversity.com/setup-development-environment-intellij-and-scala-big-data-hadoop-and-spark*/
object WordCount_cmdline_imple_v3 {
  def main(args:Array[String]): Unit ={
    //Check Argument list
    /*Program takes three inputs*/
    if(args.length != 2) {
      println("Usage: spark-submit -class<classname> <JARFILE> <execution mode><host><port>")
      System.exit(1)
    }
    //val executionmode:String=args(0).toString;
    val host:String=args(0).toString;
    val port:String=args(1).toString;
    //println(executionmode+" "+host+" "+port)
    //System.setProperty("hadoop.home.dir", "D:\\spark")
    //initializing the spark session it's an unified entry point for all spark integrations.
    //If you want to use the SQLContext,Hive Context that's available in the spark session
    val spark = SparkSession
      .builder
      .appName("StructuredSocketWordCount")
    //  .master(executionmode)
      .getOrCreate()

    //Setting the Logging level to error now so that only errors can captured.
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    //This is where stream processing start
    val lines = spark
      .readStream
      .format("socket")
      .option("host",host)
      .option("port",port)
      .load()
    //Printing schema
    lines.printSchema()

    //Split the lines into words and create a word column
    val words = lines.select(explode(split($"value"," ")).as("word"))
    //words.printSchema()
    //group the words Data frame by the word and call the count aggregation
    val wordCount =words.groupBy("word").count()
    //wordCount.printSchema()
    //Start running the query that prints the running counts to the console
    val query = wordCount.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    //It waits until program terminates
    query.awaitTermination()
  }
}
