package com.clownfsih7.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname WordCount
 * description TODO
 * create 2022-04-15 11:24 
 */
object WordCount {
  def main(args: Array[String]): Unit = {

    // 创建 spark 运行配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")

    // 创建 spark 上下文环境对象（连接对象）
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    // 读取文件数据
    val fileRDD = sparkContext.textFile("input/wd1.txt")

    // 将文件中的数据进行分词
    val wordRDD = fileRDD.flatMap(_.split(" "))

    // 转换数据结果 word => (word,1)
    val word2OneRDD = wordRDD.map((_, 1))

    // 将转换结构后的数据按照相同的单词进行分组聚合
    val word2CountRDD = word2OneRDD.reduceByKey(_ + _)

    // 将数据聚合结果采集到内存中
    val word2Count: Array[(String, Int)] = word2CountRDD.collect()

    // 打印结果
    word2Count.foreach(println)

    // 关闭 spark 连接
    sparkContext.stop()
  }
}
