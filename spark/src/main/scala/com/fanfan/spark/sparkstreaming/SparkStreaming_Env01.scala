package com.fanfan.spark.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}

object SparkStreaming_Env01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamingEnvTest")
    //创建sparkstreaming环境对象
    // TODO SparkStreaming是基于spark的模块，底层也会采用soark的环境进行封装d
    //   第二个参数表示数据采集的周期，以毫秒为单位
    val ssc = new StreamingContext(conf, Duration(3000))

    // TODO 启动采集器
    ssc.start()
    // TODO Driver程序应该等待采集器的结束而结束
    ssc.awaitTermination()

    //TODO 环境对象不能停止，如果停止，那么数据就无法采集
    //      环境对象不能停止但是main方法也不能结束，一旦结束，Driver就会结束。

    // ssc.stop()
  }
}
