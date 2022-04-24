package com.clownfish7.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * classname Demo_State
 * description TODO
 * create 2022-04-24 14:46 
 */
object Demo_State extends App {
  ssc.checkpoint("checkpoints")

  ssc.socketTextStream(ip, port)
    .flatMap(_.split(" "))
    .map((_, 1))
    //    .reduceByKey(_ + _)
    .updateStateByKey(
      // 对数据状态更新
      // param1 表示相同 key 的 value 数值
      // param2 表示缓存区相同 key 的 value 值
      (seq: Seq[Int], opt: Option[Int]) => {
        Option(opt.getOrElse(0) + seq.sum)
      }
    )
    .print()


  ssc.start()
  ssc.awaitTermination()
}
