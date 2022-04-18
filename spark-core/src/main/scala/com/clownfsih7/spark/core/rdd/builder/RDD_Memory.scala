package com.clownfsih7.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Memory
 * description TODO
 * create 2022-04-18 19:49 
 */
object RDD_Memory {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    // 从内存中创建 RDD
    val seq = Seq[Int](1, 2, 3, 4)
    val rdd = sparkContext.parallelize(seq)

    // makeRdd 底层调用就是 parallelize, param2 分区数量
    sparkContext.makeRDD(seq, 2).collect().foreach(println)

    rdd.collect().foreach(println)

    sparkContext.stop()
  }
}
