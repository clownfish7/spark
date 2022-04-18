package com.clownfsih7.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Operator_Glom
 * description 将一个分区内的数据直接转换为相同类型的内存数组进行处理，分区不变
 * create 2022-04-18 20:53 
 */
object RDD_Operator_Glom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.makeRDD(List(1, 2, 3, 4), 2)
      .glom()
      .collect()
      .foreach(data => println(data.mkString(",")))

    println("------------------------------------------")

    val sum = sparkContext.makeRDD(List(1, 2, 3, 4), 2)
      .glom()
      .map(list => list.max)
      .collect()
      .sum
    println(s"sum=$sum")


    sparkContext.stop()
  }
}
