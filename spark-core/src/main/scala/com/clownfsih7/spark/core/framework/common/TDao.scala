package com.clownfsih7.spark.core.framework.common

import com.clownfsih7.spark.core.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

/**
 * classname TDao
 * description TODO
 * create 2022-04-20 17:41 
 */
trait TDao {
  def readFile(path: String): RDD[String] = {
    EnvUtil.take().textFile(path)
  }
}
