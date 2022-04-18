package com.clownfsih7.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Operator_FlatMap
 * description TODO
 * create 2022-04-18 20:53 
 */
object RDD_Operator_FlatMap {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.makeRDD(List(
      List(1, 2), List(3, 4)
    ))
      .flatMap(list => list)
      .collect()
      .foreach(println)

    println("------------------------------------------")

    sparkContext.makeRDD(List(
      "hello scala", "hello spark"
    ))
      .flatMap(str => str.split(" "))
      .collect()
      .foreach(println)

    println("------------------------------------------")

    sparkContext.makeRDD(List(
      List(1, 2), 3, List(4, 5)
    ))
      .flatMap {
        case list: List[_] => list
        case a: Int => List(a)
      }
      .collect()
      .foreach(println)

    sparkContext.stop()
  }
}
