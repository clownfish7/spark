package com.clownfsih7.spark.core.rdd.req

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Req_C1
 * description TODO
 * create 2022-04-19 15:18 
 */
object RDD_Req_C1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.textFile("input/agent.log")
      .map(line => {
        // 时间戳，省份，城市，用户，广告
        val fields = line.split(" ")
        // (省份，广告)，1
        ((fields(1), fields(4)), 1)
      })
      .reduceByKey(_ + _)
      .map(t3 => {
        (t3._1._1, (t3._1._2, t3._2))
      })
      .groupByKey()
      .mapValues(iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      })
      .collect()
      .foreach(println)

    sparkContext.stop()
  }
}
