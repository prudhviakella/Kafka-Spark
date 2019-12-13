package spark.streaming.structured.a_WordCount

import org.apache.spark.sql.SparkSession

/*Spark Setup Intellij
* https://kaizen.itversity.com/setup-development-environment-intellij-and-scala-big-data-hadoop-and-spark*/
object WordCount_implementation_v2 {
  def main(args:Array[String]): Unit ={
    //Check Argument list
    //Note: For passing arguments in Intellij navigate to Run-->EditConfigurations in program arguments pass <hostname><port>
    //In out case arguments are localhost 9999
    //make sure to start netcat client in 9999 netcat -l -p 9999
    if(args.length != 2) {
      println("Either host name or port is missing")
      System.exit(1)
    }
    val host:String=args(0).toString;
    val port:String=args(1).toString;
    println(host+" "+port)
    System.setProperty("hadoop.home.dir", "D:\\spark")
    //initializing the spark session it's an unified entry point for all spark integrations.
    //If you want to use the SQLContext,Hive Context that's available in the spark session
    val spark = SparkSession
      .builder
      .appName("StructuredSocketWordCount")
      .master("local")
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
    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))
    // Generate running word count
    val wordCounts = words.groupBy("value").count()
    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()
  }
}
