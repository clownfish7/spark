package com.clownfish7.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * classname Demo
 * description TODO
 * create 2022-04-22 17:44 
 */
object Demo_WordCount extends App {

  ssc.socketTextStream(ip, port)
    .flatMap(_.split(" "))
    .map((_, 1))
    .reduceByKey(_ + _)
    .print()

  ssc.start()
  ssc.awaitTermination()
}
