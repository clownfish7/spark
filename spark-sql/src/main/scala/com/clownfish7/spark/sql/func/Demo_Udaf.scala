package com.clownfish7.spark.sql.func

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, functions}

/**
 * classname Demo_Udaf
 * description TODO
 * create 2022-04-21 18:22 
 */
object Demo_Udaf extends App {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  import sparkSession.implicits._

  println("----------------------------------")

  val df: DataFrame = List(1, 2, 3).toDF("num")
  df.createTempView("numTable")

  sparkSession.udf.register("avgNumOld", new AvgUdafOld)
  sparkSession.udf.register("avgNumNew", functions.udaf(new AvgUdafNew))

  sparkSession.sql("select avgNumOld(num) as old, avgNumNew(num) as new  from numTable").show

  println("----------------------------------")
  sparkSession.close()


  class AvgUdafOld extends UserDefinedAggregateFunction {
    // input
    override def inputSchema: StructType = StructType(Array(StructField("num", IntegerType)))

    // agg buff
    override def bufferSchema: StructType = StructType(Array(StructField("total", IntegerType), StructField("cnt", IntegerType)))

    // output
    override def dataType: DataType = DoubleType

    // 函数稳定性
    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, 0)
      buffer.update(1, 0)
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, buffer.getInt(0) + input.getInt(0))
      buffer.update(1, buffer.getInt(1) + 1)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getInt(0) + buffer2.getInt(0))
      buffer1.update(1, buffer1.getInt(1) + buffer2.getInt(1))
    }

    override def evaluate(buffer: Row): Double = buffer.getInt(0) / buffer.getInt(1)
  }


  case class AvgAggregate(var total: Int, var cnt: Int)

  // in buf out
  class AvgUdafNew extends Aggregator[Int, AvgAggregate, Double] {
    override def zero: AvgAggregate = AvgAggregate(0, 0)

    override def reduce(b: AvgAggregate, a: Int): AvgAggregate = {
      b.total += a
      b.cnt += 1
      b
    }

    override def merge(b1: AvgAggregate, b2: AvgAggregate): AvgAggregate = {
      b1.total += b2.total
      b1.cnt += b2.cnt
      b1
    }

    override def finish(reduction: AvgAggregate): Double = reduction.total / reduction.cnt

    // 缓冲区编码操作
    override def bufferEncoder: Encoder[AvgAggregate] = Encoders.product

    // 输出的编码操作
    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }
}


