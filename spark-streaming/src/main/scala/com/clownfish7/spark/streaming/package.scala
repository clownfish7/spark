package com.clownfish7.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * classname package
 * description TODO
 * create 2022-04-24 14:47 
 */
package object streaming {
  val ip = "192.168.0.24"
  val port = 9999
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("NAME")
  private val checkpoints = "checkpoints"
  val ssc = StreamingContext.getOrCreate(checkpoints, () => {
    val context = new StreamingContext(sparkConf, Seconds.apply(3))
    context.checkpoint("checkpoints")
    context
  })
}
