package com.sentienz.kafkaToIngestor

import com.google.gson.Gson
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{KafkaUtils, _}
import org.apache.spark.{SparkConf, TaskContext}

/**
  * @author Shubham Agrawal
  */
object KafkaToAPI {

  def main(args: Array[String]): Unit = {

    println("Usage: Interval KafkaBroker:Port TopicName ApiIP:Port")

    val conf = new SparkConf().setAppName("KafkaTopicReader_").setMaster("local[*]")

    val streamingContext = new StreamingContext(conf, Seconds(args(0).toInt))

    val kafka = args(1)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafka,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group_test_2",
      "startingOffsets" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array(args(2))
    val apiDetails = args(3)

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.foreachPartition { iter =>

        val o = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")

        iter.foreach {
          println("Message Read from Kafka")
          x =>
            println(x.value())

            x => KafkaToAPI.func(x.value(), conf, apiDetails)

        }

      }
    }

    streamingContext.start()
    streamingContext.awaitTermination()

  }

  def func(value: String, conf: SparkConf, apiDetail: String): Unit = {

    var status = 0

    val message = new Gson().fromJson(value, classOf[KafkaSampleMessage])

    val post = KafkaToAPI.getPostRequest(message, apiDetail)
    post.addHeader("Content-Type", "application/json")

    val client = HttpClients.createDefault()
    val response = client.execute(post)

    status = response.getStatusLine.getStatusCode

  }

  def getPostRequest(message: KafkaSampleMessage, apiDetail: String): HttpPost = {

    val url = "http://" + apiDetail + "/send"
    val post = new HttpPost(url)

    val msgAsJson = new Gson().toJson(message)

    post.setEntity(new StringEntity(msgAsJson))
    post
  }

}