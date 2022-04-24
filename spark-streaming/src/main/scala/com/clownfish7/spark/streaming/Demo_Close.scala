package com.clownfish7.spark.streaming

import org.apache.spark.streaming.StreamingContextState

/**
 * classname Demo_Close
 * description TODO
 * create 2022-04-24 15:32 
 */
object Demo_Close {

  new Thread(() => {
    while (true) {
      // check from other sys, maybe redis, mysql, hdfs, zookeeper
      if (true) {
        if (ssc.getState() == StreamingContextState.ACTIVE) {
          ssc.stop(true, true)
        }
      }
    }
  }).start()


  ssc.start()
  ssc.awaitTermination()
}
