package com.clownfish7.spark.streaming

import org.apache.spark.streaming.Seconds

/**
 * classname Demo_Window
 * description TODO
 * create 2022-04-24 15:14 
 */
object Demo_Window extends App {

  ssc.socketTextStream(ip, port)

    // 窗口大小，滑动步长
//    .window(Seconds(3), Seconds(3))

    .flatMap(_.split(" "))
    .map((_, 1))

    .reduceByKeyAndWindow(
      // 新窗口数据相加
      (x, y) => x + y,
      // 旧窗口数据相减
      _ - _,
      Seconds(3), Seconds(3)
    )

    .print()

  ssc.start()
  ssc.awaitTermination()
}
