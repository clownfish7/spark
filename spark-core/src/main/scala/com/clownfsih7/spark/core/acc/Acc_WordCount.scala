package com.clownfsih7.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * classname Acc_WordCount
 * description TODO
 * create 2022-04-20 11:23 
 */
object Acc_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List("hello spark", "hello scala"))

    // 自定义 acc
    val accumulator = new MyAccumulator
    sparkContext.register(accumulator, "wcAcc")

    rdd.foreach(str => {
      str.split(" ").foreach(accumulator.add)
    })

    println(accumulator.value.mkString(","))

    sparkContext.stop()
  }

  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

    private val wcMap = mutable.Map[String, Long]()

    override def isZero: Boolean = wcMap.isEmpty

    override def reset(): Unit = wcMap.clear()

    override def add(v: String): Unit = wcMap.update(v, wcMap.getOrElse(v, 0L) + 1L)

    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new MyAccumulator

    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      other.value.foreach {
        case (word: String, cnt: Long) =>
          val oldCnt = wcMap.getOrElse(word, 0L)
          wcMap.update(word, oldCnt + cnt)
      }
    }

    override def value: mutable.Map[String, Long] = wcMap
  }
}
