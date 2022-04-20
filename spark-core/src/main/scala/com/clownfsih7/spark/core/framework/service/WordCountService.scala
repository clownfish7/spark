package com.clownfsih7.spark.core.framework.service

import com.clownfsih7.spark.core.framework.common.TService
import com.clownfsih7.spark.core.framework.dao.WordCountDao
import com.clownfsih7.spark.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * classname WordCountService
 * description TODO
 * create 2022-04-20 17:44 
 */
class WordCountService extends TService {

  val dao = new WordCountDao()

  override def analysis(): Array[(String, Int)] = {
    dao.readFile("input/wd1.txt")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect()
  }
}
