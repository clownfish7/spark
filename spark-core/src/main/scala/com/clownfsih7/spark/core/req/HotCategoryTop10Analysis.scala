package com.clownfsih7.spark.core.req

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname HotCategoryTop10Analysis
 * description TODO
 * create 2022-04-20 13:44 
 */
object HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    val dataRDD = sparkContext.textFile("input/user_visit_action.txt")
      .map(line => {
        val fields = line.split("_")
        (fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7),
          fields(8), fields(9), fields(10), fields(11), fields(12))
      })
    dataRDD.cache()

    val clickRDD = dataRDD.filter("-1" != _._7)
      .map(item => (item._7, 1))
      .reduceByKey(_ + _)

    val orderRDD = dataRDD.filter("-1" != _._9)
      .map(_._9)
      .flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_ + _)

    val payRDD = dataRDD.filter("-1" != _._11)
      .map(_._11)
      .flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_ + _)

    clickRDD.cogroup(orderRDD, payRDD)
      .map(item => {
        (item._1, (item._2._1.sum, item._2._2.sum, item._2._3.sum))
      })
      .sortBy(_._2, false).take(10)
      .foreach(println)

    println("--------------------------------")

    clickRDD.map { case (cid, cnt) => (cid, (cnt, 0, 0)) }
      .union(
        orderRDD.map { case (cid, cnt) => (cid, (0, cnt, 0)) })
      .union(
        payRDD.map { case (cid, cnt) => (cid, (0, 0, cnt)) }
      )
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
      .sortBy(_._2, false).take(10)
      .foreach(println)

    sparkContext.stop()
  }
}
