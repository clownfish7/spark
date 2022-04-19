package com.clownfsih7.spark.core.rdd.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Persist
 * description 数据持久化
 * create 2022-04-19 17:29 
 */
object RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val fileRDD = sparkContext.textFile("input/wd1.txt")

    val wordRDD = fileRDD.flatMap(_.split(" "))

    val word2OneRDD = wordRDD.map(a => {
      println("@@@")
      (a, 1)
    })

    // RDD 对象重用了但数据没有重用 等同于从头又计算了一次， 耗时长的数据也可以进行持久化
    // cache 在内存中缓存一份，无需再次从头计算
    word2OneRDD.cache()
    // persist 指定存储方式
    word2OneRDD.persist(StorageLevel.DISK_ONLY)

    val reduceRDD = word2OneRDD.reduceByKey(_ + _)
    val groupRDD = word2OneRDD.groupByKey()

    reduceRDD.collect().foreach(println)
    groupRDD.collect().foreach(println)

    // 关闭 spark 连接
    sparkContext.stop()
  }
}
