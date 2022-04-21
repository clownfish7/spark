package com.clownfish7.spark.sql.func

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * classname Demo
 * description TODO
 * create 2022-04-21 18:19 
 */
object Demo_Udf extends App {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  import sparkSession.implicits._

  println("----------------------------------")

  private val df: DataFrame = List(1, 2, 3).toDF("num")
  df.createTempView("numTable")

  sparkSession.udf.register("add2", (num: Int) => num + 2)

  sparkSession.sql("select add2(num) from numTable").show

  println("----------------------------------")

  sparkSession.close()
}
