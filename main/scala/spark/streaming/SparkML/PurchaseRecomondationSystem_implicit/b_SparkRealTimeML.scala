package SparkML.PurchaseRecomondationSystem_implicit

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.functions.{current_timestamp, explode, from_json, from_unixtime, lit}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object b_SparkRealTimeML{
  def main(args:Array[String]): Unit ={
    //Setting the Logging level to error now so that only errors can captured.
    val logger = Logger.getLogger("RealTimePurchaseRecommendationEngine")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val schema = StructType(
      List(
        StructField("customerid", StringType, nullable = true),
        StructField("invoice_id", StringType, nullable = true),
        StructField("socketcode", StringType, nullable = true),
        StructField("create_ts", LongType, nullable = true),
        StructField("update_ts", LongType, nullable = true)
      )
    )

    val csv_schema =  StructType(
      List(
        StructField("InvoiceNo", StringType, nullable = true),
        StructField("StockCode", LongType, nullable = true),
        StructField("Description", StringType, nullable = true),
        StructField("Quantity", LongType, nullable = true),
        StructField("InvoiceDate", StringType, nullable = true),
        StructField("UnitPrice", DoubleType, nullable = true),
        StructField("CustomerID", LongType, nullable = true),
        StructField("Country", StringType, nullable = true)
      )
    )

    val spark = SparkSession
      .builder
      .appName("RealTimePurchaseRecommendationEngine")
      .master("local[*]")
      .getOrCreate()

    val csv_file = spark
      .read
      .option("header","true")
      .schema(csv_schema)
      .csv("/home/cloudera/spark/datasets/OnlineRetail.csv")

    import spark.implicits._
    val cleaned_df = csv_file
      .filter($"Quantity" > 0 && $"CustomerID" != 0 && $"StockCode" != 0)

    val Stage1 = cleaned_df
      .select($"CustomerID",$"Description",$"StockCode",lit(1).as("purch")).filter($"CustomerID".isNotNull && $"StockCode".isNotNull)

    import spark.implicits._
    //This is where stream processing start
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "mysql-02purchasereco")
      .option("startingOffsets", "earliest")
      .load()

    //Printing schema
    df.printSchema()
    val sameModel = ALSModel.load("purchaseRecoModel/output")
    //Split the lines into words and create a word column
    val user = df.select($"value" cast "string" as "json")
      .select(from_json($"json", schema) as "data")
      .select("data.*")
      .withColumn("create_ts",from_unixtime($"create_ts".divide(1000)))
      .withColumn("update_ts",from_unixtime($"update_ts".divide(1000)))

    user.printSchema()


    val query = user
      .writeStream
      .foreachBatch((batchDF: DataFrame, batchId: Long) =>{
        val batch = sameModel.recommendForUserSubset(batchDF.selectExpr("customerid"),5)
        batch.select($"customerid",explode($"recommendations").as("recommendations"))
          .select($"customerid",$"recommendations.StockCode",$"recommendations.rating").join(Stage1.select($"StockCode",$"Description"),Seq("StockCode"))
          .distinct()
          .withColumn("create_ts", current_timestamp())
          .withColumn("batch_id",lit(batchId))
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("url", "jdbc:mysql://localhost:3306/kafkademo")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("user", "kafka")
          .option("dbtable","Recomondations")
          .option("password", "kafka").save()
      })
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start().awaitTermination()
  }
}
