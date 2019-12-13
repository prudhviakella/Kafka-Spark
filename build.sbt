name := "Kafka-Spark"

version := "0.1"

scalaVersion := "2.11.12"


scalacOptions ++= Seq("-Xexperimental")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "joda-time" % "joda-time" % "2.10.5",
  "org.twitter4j" % "twitter4j-stream" % "4.0.7",
  "org.twitter4j" % "twitter4j-core" % "4.0.7",
  "org.apache.kafka" %% "kafka" % "2.3.1",
  "org.apache.kafka" %% "kafka-streams-scala" % "2.3.1",
  "log4j" % "log4j" % "1.2.14",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0",
  "org.apache.kafka" % "kafka-clients" % "2.3.1",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "org.scalacheck" %% "scalacheck" % "1.14.1",
  //"io.confluent" % "kafka-avro-serializer" % "5.3.0"
  "com.sksamuel.avro4s" %% "avro4s-core" % "1.9.0"
)

