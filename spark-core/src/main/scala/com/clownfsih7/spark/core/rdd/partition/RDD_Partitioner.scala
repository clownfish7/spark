package com.clownfsih7.spark.core.rdd.partition

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * classname RDD_Partitioner
 * description TODO
 * create 2022-04-19 17:50 
 */
object RDD_Partitioner {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.makeRDD(List(
      ("cba", 1), ("cba", 2), ("nba", 1), ("nba", 2), ("wba", 1)
    ), 2)
      .partitionBy(new MyPartitioner)
      .saveAsTextFile("output/partitioner")
    sparkContext.stop()
  }


  /**
   * 自定义分区器
   */
  class MyPartitioner extends Partitioner {
    // 分区数量
    override def numPartitions: Int = 3

    // 根据数据的 key 返回数据分区索引  start with 0
    override def getPartition(key: Any): Int = key match {
      case "nba" => 0
      case "cba" => 1
      case _ => 2
    }
  }
}

