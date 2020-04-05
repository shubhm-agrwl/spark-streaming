package com.sentienz.kafkaToIngestor

import javax.lang.model.`type`.ArrayType
import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import org.apache.spark.sql.types.ArrayType

object KafkaToKafka {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("test2")
      .master("local[*]")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val dfInput = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .option("startingOffsets", "latest")
      .option("group.id", "test-sling1")
      .load()

    val result_R = dfInput
      .withColumn("val", ($"value").cast("string"))
      .withColumn("msg_id", get_json_object($"val", "$.msgId"))
      .withColumn("tstamp", unix_timestamp(get_json_object($"val", "$.eventTimestamp"), "yyyy-MM-dd HH:mm:ss"))

      // Example to add String filters to the dataframe
      .filter($"msg_id" === "one" || $"type" === "two")

      // Example to add Time filter to the dataframe - last 10 minute data
      .filter((unix_timestamp(current_timestamp()) - $"tstamp") / 60 < 10)

    /*
    Example to print in console
        var df = final_ids
      .select(to_json(struct($"*")) as "value")
      .writeStream
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "/opt/checkpoint")
      .start()
     */

    // Example to use Windowing in Kafka Spark Streaming
    val final_ids = result_R
      .withWatermark("timestamp", "500 milliseconds")
      .groupBy($"msg_id", window($"timestamp", "10 seconds", "5 seconds"))

    val t = final_ids
      .select(to_json(struct($"*")) as "value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "test1")
      .option("checkpointLocation", "/opt/checkpoint")
      .outputMode(OutputMode.Update())
      .trigger(ProcessingTime("10 seconds"))
      .start()
      .awaitTermination()
  }

}
