package com.clownfsih7.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Operator_Join
 * description TODO
 * create 2022-04-19 14:32 
 */
object RDD_Operator_Join {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    val rdd1 = sparkContext.makeRDD(List(("a", 1), ("a", 3), ("b", 2), ("f", 4)), 2)
    val rdd2 = sparkContext.makeRDD(List(("a", 5), ("b", 6), ("c", 7), ("d", 8)), 2)

    println("---------------------------------------")

    rdd1.join(rdd2).collect().foreach(println)

    println("---------------------------------------")

    rdd1.leftOuterJoin(rdd2).collect().foreach(println)

    println("---------------------------------------")

    rdd1.rightOuterJoin(rdd2).collect().foreach(println)

    println("---------------------------------------")

    // connect + group
    rdd1.cogroup(rdd2).collect().foreach(println)

    println("---------------------------------------")

    sparkContext.stop()
  }
}
