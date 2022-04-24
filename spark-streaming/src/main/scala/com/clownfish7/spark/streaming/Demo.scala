package com.clownfish7.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * classname Demo
 * description TODO
 * create 2022-04-24 11:15 
 */
object Demo extends App {

  val queue = new mutable.Queue[RDD[Int]]
  ssc.queueStream(queue)
    .map((_, 1))
    .reduceByKey(_ + _)
    .print()

  ssc.start()

  for (i <- 1 to 5) {
    queue.enqueue(ssc.sparkContext.makeRDD(1 to 300, 10))
    Thread.sleep(2000)
  }

  ssc.awaitTermination()
}
