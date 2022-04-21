package com.clownfish7.spark.sql.base

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * classname Demo
 * description TODO
 * create 2022-04-21 17:41 
 */
object Demo extends App {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  // doing something...
  private val df: DataFrame = sparkSession.read.json("input/user.json")
  df.show()

  df.createTempView("user")
  sparkSession.sql("select * from user").show()
  sparkSession.sql("select * from user").show(5)

  println("----------------------------------")

  // dataFrame -> dsl
  df.select("username", "age").show()

  // 在使用 dataFrame 时，如果设计到转换操作需要引入转换规则

  import sparkSession.implicits._

  df.select($"age" + 1).show

  private val ds: Dataset[Int] = List(1, 2, 3).toDS()
  ds.show()

  println("----------------------------------")

  private val rdd: RDD[(Int, String, Int)] = sparkSession.sparkContext.makeRDD(List((1, "zhangsan", 18), (2, "lisi", 19), (3, "wangwu", 20)))

  private val dataFrame: DataFrame = rdd.toDF("id", "name", "age")

  private val backRdd: RDD[Row] = dataFrame.rdd

  println("----------------------------------")

  private val dataSet: Dataset[User] = dataFrame.as[User]

  private val backDataFrame: DataFrame = dataSet.toDF()

  println("----------------------------------")

  private val rdd2DataSet1: Dataset[(Int, String, Int)] = rdd.toDS()

  private val rdd2DataSet2: Dataset[User] = rdd.map {
    case (id, name, age) => User(id, name, age)
  }.toDS()

  private val backRDD1: RDD[(Int, String, Int)] = rdd2DataSet1.rdd
  private val backRDD2: RDD[User] = rdd2DataSet2.rdd

  println("----------------------------------")

  sparkSession.close()

  case class User(id: Int, name: String, age: Int)
}
