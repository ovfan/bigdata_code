package com.fanfan.flink.scala.demos

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}

import scala.collection.TraversableOnce

object WordCount01 {
  def main(args: Array[String]): Unit = {
    // 获取环境对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并发度
    env.setParallelism(2)

    // 获取socket源数据
    val words: DataStream[String] = env.socketTextStream("192.168.202.102", 9999)

    // 处理数据--scala处理数据时，会有个隐式参数，在导包时加上 createTypeInformation即可
    words
      .flatMap(s => {
        s.split("\\s+").map(w => (w, 1))
      })
      .keyBy(tp => tp._1)
      .sum(1)
      .print()
    // 触发环境执行
    env.execute()

  }
}
