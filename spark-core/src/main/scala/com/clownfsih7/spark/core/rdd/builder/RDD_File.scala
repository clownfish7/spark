package com.clownfsih7.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_File
 * description TODO
 * create 2022-04-18 19:55 
 */
object RDD_File {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    // 从文件中创建 RDD
    // path 可以是绝对路径 / 相对路径 / 也可以是目录 / * 通配符 / hdfs
    sparkContext.textFile("input").collect().foreach(println)

    // 以文件为单位读取数据
    sparkContext.wholeTextFiles("input").collect().foreach(println)

    sparkContext.stop()
  }
}
