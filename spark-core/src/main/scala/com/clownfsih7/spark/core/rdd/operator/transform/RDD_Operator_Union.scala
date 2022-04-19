package com.clownfsih7.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Operator_Union
 * description TODO
 * create 2022-04-19 11:41 
 */
object RDD_Operator_Union {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    val rdd1 = sparkContext.makeRDD(List(1, 2, 3, 4), 2)
    val rdd2 = sparkContext.makeRDD(List(3, 4, 5, 6), 2)

    println("---------------------------------------")

    // 交集 数据类型需一致
    println(rdd1.intersection(rdd2).collect().mkString(","))

    println("---------------------------------------")

    // 并集 数据类型需一致
    println(rdd1.union(rdd2).collect().mkString(","))

    println("---------------------------------------")

    // 差集 数据类型需一致
    println(rdd1.subtract(rdd2).collect().mkString(","))
    println(rdd2.subtract(rdd1).collect().mkString(","))

    println("---------------------------------------")

    // 拉链 分区数量需相等，分区内数据数量需一致
    println(rdd1.zip(rdd2).collect().mkString(","))

    sparkContext.stop()
  }
}
