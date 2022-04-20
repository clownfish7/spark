package com.clownfsih7.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname RDD_IO_Save
 * description TODO
 * create 2022-04-20 11:01 
 */
object RDD_IO_Save {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(("a", 1), ("b", 2)))

    rdd.saveAsTextFile("output/save/text")
    rdd.saveAsObjectFile("output/save/obj")
    rdd.saveAsSequenceFile("output/save/seq")

    sparkContext.textFile("")
    sparkContext.objectFile[(String, Int)]("")
    sparkContext.sequenceFile[String, Int]("")
    sparkContext.hadoopFile("")


    sparkContext.stop()
  }
}
