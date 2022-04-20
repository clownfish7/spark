package com.clownfsih7.spark.core.acc

import com.clownfsih7.spark.core.acc.Acc_WordCount.MyAccumulator
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname BoardCast
 * description TODO
 * create 2022-04-20 13:34 
 */
object BroadCast {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    val words = List("hello spark", "hello scala")
    // 广播变量
    val value = sparkContext.broadcast(words)

    sparkContext.stop()
  }
}
