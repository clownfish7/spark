package com.clownfsih7.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Operator_SortBy
 * description TODO
 * create 2022-04-19 11:36 
 */
object RDD_Operator_SortBy {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.makeRDD(List(1, 3, 2, 4, 6, 5), 3)
      .sortBy(i => i)
      .collect()
      .foreach(println)

    println("---------------------------------------")

    sparkContext.makeRDD(List(1, 3, 2, 4, 6, 5), 3)
      // 降序
      .sortBy(i => i,false)
      .collect()
      .foreach(println)

    sparkContext.stop()
  }
}
