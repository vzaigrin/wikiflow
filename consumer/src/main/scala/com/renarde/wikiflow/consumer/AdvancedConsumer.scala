package com.renarde.wikiflow.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}

object AdvancedConsumer extends App with LazyLogging {
  val appName: String = "advanced-consumer"

  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .config("spark.driver.memory", "5g")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  logger.info("Initializing Structured consumer")

  val inputStream: DataFrame = spark
    .readStream
    .format("delta")
    .load("/storage/analytics-consumer/output")

  val consoleOutput = inputStream
    .writeStream
    .outputMode("append")
    .format("console")
    .start()

  spark.streams.awaitAnyTermination()
}
