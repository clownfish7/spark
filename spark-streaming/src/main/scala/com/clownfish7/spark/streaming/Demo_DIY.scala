package com.clownfish7.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

import scala.util.Random

/**
 * classname Demo_DIY
 * description TODO
 * create 2022-04-24 11:21 
 */
object Demo_DIY extends App {
  ssc.receiverStream(new MyReceiver)
    .print()

  ssc.start()
  ssc.awaitTermination()

  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {

    @volatile var flag = true

    override def onStart(): Unit = {
      new Thread(() => {
        while (true) {
          val message = "message: " + new Random().nextInt(10).toString
          store(message)
          Thread.sleep(500)
        }
      }).start()
    }

    override def onStop(): Unit = {
      flag = false
    }
  }
}
