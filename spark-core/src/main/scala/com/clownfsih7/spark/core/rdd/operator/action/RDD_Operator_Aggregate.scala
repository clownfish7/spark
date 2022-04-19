package com.clownfsih7.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Operator_Reduce
 * description TODO
 * create 2022-04-19 15:41 
 */
object RDD_Operator_Aggregate {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(1, 3, 5, 2, 4, 6), 3)

    println("---------------------------------------")

    println(rdd.aggregate(0)(_ + _, _ + _))

    println("---------------------------------------")

    println(rdd.fold(0)(_ + _))

    println("---------------------------------------")

    sparkContext.stop()
  }
}
