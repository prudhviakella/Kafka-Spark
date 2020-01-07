package SparkML.movierecomondations_explicit

import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.{DataFrame, SparkSession}

object b_SparkRealTimeML{
  def main(args:Array[String]): Unit ={
    val spark = SparkSession
      .builder
      .appName("StructuredSocketWordCount")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    //This is where stream processing start
    val lines = spark
      .readStream
      .format("socket")
      .option("host","localhost")
      .option("port","9999")
      .load()
    //Printing schema
    lines.printSchema()
    val sameModel = ALSModel.load("target/tmp/myCollaborativeFilter")
    //Split the lines into words and create a word column
    val user = lines.select($"value".as("userID"))

    user.printSchema()
    //Setting the Logging level to error now so that only errors can captured.
    spark.sparkContext.setLogLevel("ERROR")

    val query = user
      .writeStream
      .foreachBatch((batchDF: DataFrame, batchId: Long) =>{
        sameModel.recommendForUserSubset(batchDF.selectExpr("userID"),5).show(10)
      })
      .outputMode("append")
      .start().awaitTermination()
  }
}
