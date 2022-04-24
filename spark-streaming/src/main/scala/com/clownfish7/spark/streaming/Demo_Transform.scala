package com.clownfish7.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * classname Demo_Transform
 * description TODO
 * create 2022-04-24 15:01 
 */
object Demo_Transform extends App {

  private val source: ReceiverInputDStream[String] = ssc.socketTextStream(ip, port)

  // 当 stream 不能满足需求时可以使用 transform

  // code 1   driver 端执行
  source
    .transform(rdd => {
      // code2   driver 端执行  周期性
      rdd.map(str => {
        // code3   executor 端执行  周期性
        str
      })
    })

  // code 1   driver 端执行
  source
    .map(str => {
      // code3   executor 端执行  周期性
      str
    })

  ssc.start()
  ssc.awaitTermination()
}
