package com.clownfsih7.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname ACC
 * description TODO
 * create 2022-04-20 11:08 
 */
object Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4))

    var sum = 0

    rdd.foreach(num => sum += num)
    // res = 0
    println(s"sum = $sum")

    // 累加器  没有行动算子不触发累加器 会少加  多行动算子会多次执行 会多加
    // 一般情况下累加器放在行动算子中
    val sumAcc = sparkContext.longAccumulator("sum")
    rdd.foreach(sumAcc.add(_))
    // res = 10
    println(s"sumAcc = ${sumAcc.value}")

    sparkContext.stop()
  }
}
