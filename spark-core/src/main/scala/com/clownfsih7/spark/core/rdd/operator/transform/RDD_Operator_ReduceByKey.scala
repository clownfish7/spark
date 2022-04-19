package com.clownfsih7.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Operator_ReduceByKey
 * description TODO
 * create 2022-04-19 13:43 
 */
object RDD_Operator_ReduceByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 1), ("b", 2), ("b", 3)), 3)
      // 如果 key 的数据只有一个不进行计算
      .reduceByKey(_ + _)
      .collect()
      .foreach(println)

    println("---------------------------------------")


    sparkContext.stop()
  }
}
