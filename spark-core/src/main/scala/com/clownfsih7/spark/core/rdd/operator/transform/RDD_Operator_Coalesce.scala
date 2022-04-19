package com.clownfsih7.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Operator_Coalesce
 * description 缩减分区
 * create 2022-04-19 11:23 
 */
object RDD_Operator_Coalesce {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
      // 默认不会 shuffle
      .coalesce(2, true)
      .saveAsTextFile("output/coalesce")

    println("---------------------------------------")

    sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
      // 扩大分区需要 shuffle
      .coalesce(3, true)
      .saveAsTextFile("output/coalesce2")

    sparkContext.stop()
  }
}
