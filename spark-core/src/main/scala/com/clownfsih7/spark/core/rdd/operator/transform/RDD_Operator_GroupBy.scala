package com.clownfsih7.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Operator_GroupBy
 * description TODO
 * create 2022-04-18 21:26 
 */
object RDD_Operator_GroupBy {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.makeRDD(List(1, 2, 3, 4), 2)
      .groupBy(i => i % 2)
      .collect()
      .foreach(println)

    println("------------------------------------------")

    sparkContext.makeRDD(List("Hello", "Hadoop", "Scala", "Spark"), 2)
      .groupBy(s => s.charAt(0))
      .collect()
      .foreach(println)

    sparkContext.stop()
  }
}
