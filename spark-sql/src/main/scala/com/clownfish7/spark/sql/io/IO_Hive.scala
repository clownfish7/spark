package com.clownfish7.spark.sql.io

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * classname IO_Hive
 * description TODO
 * create 2022-04-22 15:27 
 */
object IO_Hive extends App {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  import sparkSession.implicits._

  // 使用 SparkSql 连接外置 hive
  // 1. 拷贝 hive-site.xml 到 classpath
  // 2. 启用 hive 支持
  // 3. 增加对应的依赖关系  包含 mysql 驱动

  sparkSession.sql("show tables").show()

  sparkSession.close()
}
