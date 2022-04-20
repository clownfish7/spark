package com.clownfsih7.spark.core.framework.common

import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname TApplication
 * description TODO
 * create 2022-04-20 17:46 
 */
trait TApplication {
  def start(master: String = "local[*]", app: String = "Application")(op: => Unit): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster(master).setAppName(app)
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }
    sparkContext.stop()
  }
}
