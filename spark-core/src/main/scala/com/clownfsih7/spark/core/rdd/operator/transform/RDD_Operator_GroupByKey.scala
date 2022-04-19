package com.clownfsih7.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Operator_GroupByKey
 * description TODO
 * create 2022-04-19 13:46 
 */
object RDD_Operator_GroupByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 1), ("b", 2), ("b", 3)), 3)
      .groupByKey()
      .collect()
      .foreach(println)

    println("---------------------------------------")


    sparkContext.stop()
  }
}
