package SparkML.PurchaseRecomondationSystem_implicit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.ml.recommendation.ALS
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator

object a_SparkBatchML {
  def main(args:Array[String]): Unit ={

    val logger = Logger.getLogger("SparkRetailRecomondationEngine")
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val schema =  StructType(
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
                .builder()
                .appName("SparkRetailRecomondationEngine")
                .master("local[*]")
                .getOrCreate()
    val csv_file = spark
                   .read
                   .option("header","true")
                   .schema(schema)
                   .csv("/home/cloudera/spark/datasets/OnlineRetail.csv")
    import spark.implicits._
    val cleaned_df = csv_file
      .filter($"Quantity" > 0 && $"CustomerID" != 0 && $"StockCode" != 0)

    val Stage1 = cleaned_df
      .select($"CustomerID",$"StockCode",lit(1).as("purch")).filter($"CustomerID".isNotNull && $"StockCode".isNotNull)

    //Stage1.show(10)
    /*Spliting Data into Tanning,Test,CrossValidation*/
    val Array(testDf,cvDf,trainDf) = Stage1.randomSplit(Array(.1, .1,.8))
    /*Tanning Model*/
//    val als1 = new ALS().setImplicitPrefs(true).setMaxIter(15).setRegParam(0.1).setUserCol("CustomerID").setItemCol("StockCode").setRatingCol("purch").setColdStartStrategy("drop")
//    val model1 = als1.fit(trainDf)
//    val als2 = new ALS().setImplicitPrefs(true).setMaxIter(3).setRegParam(0.1).setUserCol("CustomerID").setItemCol("StockCode").setRatingCol("purch").setColdStartStrategy("drop")
//    val model2 = als2.fit(trainDf)
    val als3 = new ALS().setImplicitPrefs(true).setRank(15).setMaxIter(15).setRegParam(0.1).setUserCol("CustomerID").setItemCol("StockCode").setRatingCol("purch").setColdStartStrategy("drop")
    val model3 = als3.fit(trainDf)

    println("The models are trained")
    /*Testing*/
    /*Cross Validation*/
    /*On collect the data will convert into List[Row] to convert to desired format need to cast using as[Long]*/
    val customers = trainDf.select($"CustomerID").distinct().as[Long].collect()
    val stock =  trainDf.select($"StockCode").distinct().as[String].collect()
    val cvdf_count = cvDf.filter($"CustomerID".isin(customers: _*) && $"StockCode".isin(stock: _*))
//    println(cvDf.count)
//    println(cvdf_count.count)
    /*Run the models on the cross-validation data set.*/
    println("Model Tranning started")
//    val predictions1 = model1.transform(cvDf)
//    val predictions2 = model2.transform(cvDf)
    val predictions3 = model3.transform(cvDf)
    println("Calculating the performance of Model: Root Mean Squared Error")
//    val meansquare_df1 = predictions1.withColumn("meansqr",sqrt($"purch" - $"prediction")).filter(!$"meansqr".isNaN)
//    meansquare_df1.describe().show()
//    val meansquare_df2 = predictions2.withColumn("meansqr",sqrt($"purch" - $"prediction")).filter(!$"meansqr".isNaN)
//    meansquare_df2.describe().show()
//    val meansquare_df3 = predictions3.withColumn("meansqr",sqrt($"purch" - $"prediction")).filter(!$"meansqr".isNaN)
//    meansquare_df3.describe().show()
    /*which ever is giving is the low score that is the best model*/
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("purch")
      .setPredictionCol("prediction")
//    val rmse = evaluator.evaluate(predictions1)
//    println(s"Root-mean-square error model1 = $rmse")
//    val rmse2 = evaluator.evaluate(predictions2)
//    println(s"Root-mean-square error model2 = $rmse2")
    val rmse3 = evaluator.evaluate(predictions3)
    println(s"Root-mean-square error model3 = $rmse3")
    /*Choose the best model and  and train model with trained*/
    val filteredTestDF = trainDf.filter($"CustomerID".isin(customers: _*) && $"StockCode".isin(stock: _*))
    val predictions4 = model3.transform(filteredTestDF)
    val rmse4 = evaluator.evaluate(predictions4)
    println(s"Root-mean-square error model3,prediction4 = $rmse4")
    println("Get top 3 movie recommendations for all users Step  5....................................................")
    val userRecsAll = model3.recommendForAllUsers(3)
    userRecsAll.show()
    userRecsAll.filter($"CustomerID".equalTo(14450)).select($"CustomerID",explode($"recommendations").as("recommendations"))
      .select($"CustomerID",$"recommendations.StockCode",$"recommendations.rating").join(csv_file.select($"StockCode",$"Description"),Seq("StockCode"))
      .distinct().show(false)
    //Saving Model output to Folder
    model3.save("purchaseRecoModel/output")
    spark.close()
  }
}
