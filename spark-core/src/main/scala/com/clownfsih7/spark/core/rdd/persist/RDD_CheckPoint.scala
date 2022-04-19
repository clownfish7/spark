package com.clownfsih7.spark.core.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

/**
 * classname RDD_CheckPoint
 * description TODO
 * create 2022-04-19 17:36 
 */
object RDD_CheckPoint {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    // checkPoint path
    sparkContext.setCheckpointDir("output/checkpoint")

    val fileRDD = sparkContext.textFile("input/wd1.txt")

    val wordRDD = fileRDD.flatMap(_.split(" "))

    val word2OneRDD = wordRDD.map(a => {
      println("@@@")
      (a, 1)
    })

    // checkPoint 会独立执行一次，so 先缓存一份数据
    word2OneRDD.cache()
    word2OneRDD.checkpoint()

    val v1 = word2OneRDD.reduceByKey(_ + _)
    v1.collect().foreach(println)
    println("-----------------")
    val v2 = word2OneRDD.groupByKey()
    v2.collect().foreach(println)

    // 关闭 spark 连接
    sparkContext.stop()
  }
}
