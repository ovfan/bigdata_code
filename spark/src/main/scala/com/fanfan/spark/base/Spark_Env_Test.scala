package com.fanfan.spark.base

import org.apache.spark.{SparkConf, SparkContext}

object Spark_Env_Test {
  def main(args: Array[String]): Unit = {
    //TODO 测试spark的环境--演示第一个spark程序
    // 计算逻辑必须获取spark的环境对象(上下文对象)，才可以利用soark框架进行分布式计算

    // 构建spark环境配置对象
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    // 构建环境对象
    val sc = new SparkContext(conf)

    // 释放环境对象
    sc.stop()

  }
}
