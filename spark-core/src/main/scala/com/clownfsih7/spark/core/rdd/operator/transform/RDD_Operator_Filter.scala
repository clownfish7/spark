package com.clownfsih7.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Operator_Filter
 * description TODO
 * create 2022-04-19 10:29 
 */
object RDD_Operator_Filter {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.makeRDD(List(1, 2, 3, 4))
      .filter(_ % 2 == 0)
      .collect()
      .foreach(println)

    sparkContext.stop()
  }
}
