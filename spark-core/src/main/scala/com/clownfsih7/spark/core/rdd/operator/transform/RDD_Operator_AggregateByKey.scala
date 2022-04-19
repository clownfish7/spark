package com.clownfsih7.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_Operator_AggregateByKey
 * description TODO
 * create 2022-04-19 13:57 
 */
object RDD_Operator_AggregateByKey {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    sparkContext.makeRDD(List(
      ("a", 1), ("a", 3), ("b", 2), ("b", 4)), 2)
      // 初始值
      .aggregateByKey(0)(
        // 分区内计算 math.max
        (x, y) => math.max(x, y),
        // 分区间计算 _ + _
        (x, y) => x + y
      )
      .collect()
      .foreach(println)

    println("---------------------------------------")

    // 如果分区内 分区间计算规则相同使用简化方法 foldByKey
    sparkContext.makeRDD(List(
      ("a", 1), ("a", 3), ("b", 2), ("b", 4)), 2)
      .foldByKey(0)(_ + _)
      .collect()
      .foreach(println)

    println("---------------------------------------")

    sparkContext.makeRDD(List(
      ("a", 1), ("a", 3), ("b", 2), ("b", 4)), 2)
      .combineByKey(
        // 相同 key 的第一个值进行转换    (sum,cnt)
        (_, 1),
        // 分区内计算
        (t: (Int, Int), i: Int) => (t._1 + i, t._2 + 1),
        // 分区间计算
        (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2)
      )
      .collect()
      .foreach(println)


    sparkContext.stop()
  }
}
