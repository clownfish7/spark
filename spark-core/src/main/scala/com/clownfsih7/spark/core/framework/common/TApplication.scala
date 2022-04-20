package com.clownfsih7.spark.core.framework.common

import com.clownfsih7.spark.core.framework.util.EnvUtil
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
    EnvUtil.put(sparkContext)
    try {
      op
    } catch {
      case ex: Throwable => println(ex.getMessage)
    }
    EnvUtil.clear()
    sparkContext.stop()
  }
}
