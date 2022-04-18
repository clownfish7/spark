package com.clownfsih7.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Operator_Map
 * description TODO
 * create 2022-04-18 20:27 
 */
object RDD_Operator_Map {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    // 挨个处理
    sparkContext.makeRDD(List(1, 2, 3, 4))
      .map(_ * 2)
      .collect()
      .foreach(println)

    println("------------------------------------------")

    // 缓冲区 一次性拿取一个分区的数据进行处理，数据大有内存溢出风险
    sparkContext.makeRDD(List(1, 2, 3, 4), 2)
      .mapPartitions(iter => {
        List(iter.max).iterator
      })
      .collect()
      .foreach(println)

    println("------------------------------------------")

    // 分区编号
    sparkContext.makeRDD(List(1, 2, 3, 4), 2)
      .mapPartitionsWithIndex((index, iter) => {
        if (1 == index) iter else Nil.iterator
      })
      .collect()
      .foreach(println)

    println("------------------------------------------")

    sparkContext.makeRDD(List(1, 2, 3, 4), 8)
      .mapPartitionsWithIndex((index, iter) => {
        iter.map(num => (index, num))
      })
      .collect()
      .foreach(println)

    sparkContext.stop()
  }
}
