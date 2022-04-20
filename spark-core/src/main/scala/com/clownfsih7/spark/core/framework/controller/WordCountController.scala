package com.clownfsih7.spark.core.framework.controller

import com.clownfsih7.spark.core.framework.common.TController
import com.clownfsih7.spark.core.framework.service.WordCountService

/**
 * classname WordCountController
 * description TODO
 * create 2022-04-20 17:42 
 */
class WordCountController extends TController {

  private val wordCountService = new WordCountService

  def exec(): Unit = {
    wordCountService.analysis()
      .foreach(println)
  }
}
