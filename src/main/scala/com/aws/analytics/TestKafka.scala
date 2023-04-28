package com.aws.analytics

import org.apache.spark.sql.SparkSession

object TestKafka {
  //  System.setProperty("aws.accessKeyId", "asdfgads")
  //  System.setProperty("aws.secretKey", "/asdf")
  //  val sparkConf = new SparkConf()
  def main(args: Array[String]): Unit = {
    val AWS_ENDPOINT = "s3.amazonaws.com"
    val classpath = System.getProperty("java.class.path")
    println(s"Classpath: $classpath")
    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val bootstrapServers = "b-6.msk06.dr04w4.c3.kafka.ap-southeast-1.amazonaws.com:9092,b-5.msk06.dr04w4.c3.kafka.ap-southeast-1.amazonaws.com:9092,b-1.msk06.dr04w4.c3.kafka.ap-southeast-1.amazonaws.com:9092"


    val kafkaProperties = Map(
      "kafka.bootstrap.servers" -> bootstrapServers,
      "subscribe" -> "logevent",
      //"kafka.security.protocol" -> "SASL_SSL",
      //"kafka.sasl.mechanism" -> "AWS_MSK_IAM",
      //"kafka.sasl.client.callback.handler.class" -> "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
      //"kafka.sasl.jaas.config" -> "software.amazon.msk.auth.iam.IAMLoginModule required;"
    )

    import spark.implicits._

    val df = spark.read
      .format("kafka")
      .options(kafkaProperties)
      .load().limit(20)
      .selectExpr("CAST(value AS STRING)")
      .as[String]
    spark.read.json(df).show(100)

  }
}