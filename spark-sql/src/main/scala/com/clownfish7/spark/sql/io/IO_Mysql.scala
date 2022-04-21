package com.clownfish7.spark.sql.io

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

/**
 * classname IO_Mysql
 * description TODO
 * create 2022-04-21 20:39 
 */
object IO_Mysql extends App {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  import sparkSession.implicits._

  println("----------------------------------")

  private val properties = new Properties()
  properties.put("user", "user")
  properties.put("password", "pwd")
  properties.put("driver", "com.mysql.jdbc.Driver")
  private val df: DataFrame = sparkSession.read.jdbc("jdbc:mysql://ip:port/database", "table", properties)
  df.show

  println("----------------------------------")

  df.write.jdbc("jdbc:mysql://ip:port/database", "table2", properties)

  //  sparkSession.read.json("")
  //  df.write.json("")
  sparkSession.close()
}
