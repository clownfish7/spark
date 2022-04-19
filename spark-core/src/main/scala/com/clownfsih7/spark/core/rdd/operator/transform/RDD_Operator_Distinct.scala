package com.clownfsih7.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Operator_Distinct
 * description TODO
 * create 2022-04-19 11:16 
 */
object RDD_Operator_Distinct {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.makeRDD(List(1, 2, 3, 4, 3, 2, 1))
      .distinct
      .collect()
      .foreach(println)

    sparkContext.stop()
  }
}
