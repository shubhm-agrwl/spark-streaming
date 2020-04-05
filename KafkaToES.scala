package com.sentienz.kafkaToIngestor

import org.apache.spark.{SparkContext}
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, TaskContext}
import com.google.gson.Gson
import org.apache.http.entity.StringEntity
import java.util.Base64
import org.elasticsearch.spark.rdd.EsSpark._

/**
  * @author Shubham Agrawal
  */
object KafkaToES {

  def main(args: Array[String]): Unit = {

    println("Usage: ESNode ESPort Interval KafkaBroker:Port TopicName")

    val conf = new SparkConf().setAppName("KafkaTopicReader_").setMaster("local[*]")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes.wan.only", "true")
    conf.set("es.nodes", args(0))
    conf.set("es.port", args(1))
    val streamingContext = new StreamingContext(conf, Seconds(args(2).toInt));
    val kafka = args(3)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafka,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group_test_2",
      "startingOffsets" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array(args(4))

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>

        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")

        iter.foreach {
          println("Message Read from Kafka");

          x => KafkaIngestor.func(x.value(), conf)
        }

      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()

  }

  def func(value: String, conf: SparkConf): Unit = {

    val valueES = value

    val sparkContext = SparkContext.getOrCreate(conf)
    val messsageRDD = sparkContext.parallelize(Seq(valueES));
    saveJsonToEs(messsageRDD, "spark/mem")

  }

}


