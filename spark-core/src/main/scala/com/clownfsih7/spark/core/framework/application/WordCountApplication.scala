package com.clownfsih7.spark.core.framework.application

import com.clownfsih7.spark.core.framework.common.TApplication
import com.clownfsih7.spark.core.framework.controller.WordCountController

/**
 * classname WordCountApplication
 * description TODO
 * create 2022-04-20 17:14 
 */
object WordCountApplication extends App with TApplication {
  start() {
    val wordCountController = new WordCountController
    wordCountController.exec()
  }
}
