package com.sentienz.kafkaToIngestor

import java.sql.DriverManager
import java.util.{Base64, Properties}
import com.google.gson.Gson
import org.apache.http.entity.StringEntity
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{KafkaUtils, _}
import org.apache.spark.{SparkConf, TaskContext}

/**
  * @author Shubham Agrawal
  */
object KafkaToMySql {

  def main(args: Array[String]): Unit = {

    println("Usage: Interval KafkaBroker:Port TopicName")

    val conf = new SparkConf().setAppName("KafkaTopicReader_").setMaster("local[*]")

    val streamingContext = new StreamingContext(conf, Seconds(args(0).toInt));

    val kafka = args(1)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafka,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group_test_2",
      "startingOffsets" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array(args(2))

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.foreachPartition { iter =>

        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        Class.forName("com.mysql.jdbc.Driver")

        // MySql Connection

        val conn = DriverManager.getConnection("jdbc:mysql://localhost", "username", "password")

        val stmt = conn.prepareStatement("INSERT INTO testDF VALUES (?,?) ")

        // this configuration enables you to set autocommit, change to True if not required
        conn.setAutoCommit(false)
        val stmt = conn.createStatement()

        iter.foreach {
          println("Message Read from Kafka");
          x =>
            println(x.value())

            val message = new Gson().fromJson(value, classOf[KafkaSampleMessage])

            stmt.setString(1, message.msgId)
            stmt.setLong(2, message.eventTimeStamp)
            stmt.executeUpdate

            // this statement can be used to commit batches of statements instead of one
            conn.commit()

        }

      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()

  }

}