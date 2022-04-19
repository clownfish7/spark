package com.clownfsih7.spark.core.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * classname RDD_Operator_PartitionBy
 * description TODO
 * create 2022-04-19 11:50 
 */
object RDD_Operator_PartitionBy {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4), 2)

    println("---------------------------------------")

    // 根据分区规则对数据进行重分区
    rdd.map((_, 1))
      .partitionBy(new HashPartitioner(2))
      .saveAsTextFile("output/partitionBy")

    println("---------------------------------------")

    sparkContext.stop()
  }
}
