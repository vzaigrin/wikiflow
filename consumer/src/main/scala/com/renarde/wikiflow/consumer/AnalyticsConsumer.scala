package com.renarde.wikiflow.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.renarde.wikiflow.consumer.data.expectedSchema
import org.apache.spark.sql.types.TimestampType

object AnalyticsConsumer extends App with LazyLogging {
  val appName: String = "analytics-consumer-example"

  val spark: SparkSession = SparkSession.builder()
    .appName(appName)
    .config("spark.driver.memory", "5g")
    .config("spark.sql.streaming.checkpointLocation", "/storage/analytics-consumer/checkpoints")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  spark.sparkContext.setLogLevel("WARN")

  logger.info("Initializing Structured consumer")

  val inputStream: DataFrame = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "wikiflow-topic")
    .option("startingOffsets", "earliest")
    .load()

  val transformedStream: DataFrame = inputStream
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    .filter($"value".isNotNull)
    .select(from_json($"value", expectedSchema).as("data"))
    .select("data.*")
    .filter($"bot" =!= true)
    .withColumn("timestamp", $"timestamp".cast(TimestampType))
    .withWatermark("timestamp", "10 minutes")
    .groupBy(window($"timestamp", "10 minutes", "5 minutes"), $"type")
    .count
    .withColumn("timestamp", current_timestamp())

  transformedStream.writeStream
    .format("delta")
    .outputMode("append")
    .start("/storage/analytics-consumer/output")

  spark.streams.awaitAnyTermination()
}
