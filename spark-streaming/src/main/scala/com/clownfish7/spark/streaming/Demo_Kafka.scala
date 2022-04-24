package com.clownfish7.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
 * classname Demo_Kafka
 * description TODO
 * create 2022-04-24 14:01 
 */
object Demo_Kafka extends App {

  val kafkaParam = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "",
    ConsumerConfig.GROUP_ID_CONFIG -> "",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
  )

  private val kafkaDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](Set("topic"), kafkaParam)
  )

  kafkaDS.map(_.value()).print()

  ssc.start()
  ssc.awaitTermination()
}
