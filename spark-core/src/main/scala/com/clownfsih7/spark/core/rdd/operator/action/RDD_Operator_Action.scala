package com.clownfsih7.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Operator_Action
 * description TODO
 * create 2022-04-19 15:39 
 */
object RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(1, 3, 5, 2, 4, 6), 3)
    val reduce = rdd.reduce(_ + _)
    println(reduce)

    println("---------------------------------------")

    println(rdd.count())

    println("---------------------------------------")

    println(rdd.first())

    println("---------------------------------------")

    println(rdd.take(3).mkString(","))

    println("---------------------------------------")

    println(rdd.takeOrdered(3).mkString(","))

    println("---------------------------------------")

    println(rdd.takeOrdered(3)(Ordering.Int.reverse).mkString(","))

    println("---------------------------------------")

    sparkContext.stop()
  }
}
