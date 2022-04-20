package com.clownfsih7.spark.core.framework.util

import org.apache.spark.SparkContext

/**
 * classname EnvUtil
 * description TODO
 * create 2022-04-20 17:55 
 */
object EnvUtil {
  private val threadLocal = new ThreadLocal[SparkContext]()

  def put(context: SparkContext): Unit = {
    threadLocal.set(context)
  }

  def take(): SparkContext = {
    threadLocal.get()
  }
}
