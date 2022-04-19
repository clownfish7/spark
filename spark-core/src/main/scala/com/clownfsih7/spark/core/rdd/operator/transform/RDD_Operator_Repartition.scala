package com.clownfsih7.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Operator_Repartitions
 * description TODO
 * create 2022-04-19 11:30 
 */
object RDD_Operator_Repartition {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
      // 底层就是 colaesce
      .repartition(3)
      .saveAsTextFile("output/repartition")

    sparkContext.stop()
  }
}
