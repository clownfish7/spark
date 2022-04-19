package com.clownfsih7.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Operator_Sample
 * description TODO
 * create 2022-04-19 10:30 
 */
object RDD_Operator_Sample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    val dataRdd = sparkContext.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    println(
      dataRdd
        // parma1 抽取数据是否放回
        // param2 每条数据抽取概率
        // param3 抽取随机算法种子
        .sample(false, 0.4, 1)
        .collect()
        .mkString(",")
    )

    println("------------------------------------------")

    println(
      dataRdd
        // parma1 抽取数据是否放回
        // param2 每条数据抽取概率
        // param3 抽取随机算法种子
        .sample(true, 0.4 , 1)
        .collect()
        .mkString(",")
    )

    sparkContext.stop()
  }
}
