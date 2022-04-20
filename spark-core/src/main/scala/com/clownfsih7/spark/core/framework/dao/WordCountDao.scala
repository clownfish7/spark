package com.clownfsih7.spark.core.framework.dao

import com.clownfsih7.spark.core.framework.common.TDao
import com.clownfsih7.spark.core.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

/**
 * classname WordCountDao
 * description TODO
 * create 2022-04-20 17:44 
 */
class WordCountDao extends TDao {
  override def readFile(path: String): RDD[String] = {
    EnvUtil.take().textFile(path)
  }
}
