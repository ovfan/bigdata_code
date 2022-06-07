package com.fanfan.spark.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Parallelism {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    // 配置对象中设置分区数量
    conf.set("spark.default.parallelism", "6")

    val sc = new SparkContext(conf)
    // RDD分区数量的判断依据

    // 1. 手动设置分区数量

    val rdd: RDD[Int] = sc.makeRDD(Seq(1, 2, 3, 4, 5, 6), 3) // 设置3个分区，分区才能提高计算的并行度
    // 优先级 是 方法中设置分区 > 配置对象

    // 验证：将数据分区保存成文本文件
    rdd.saveAsTextFile("spark/src/main/resources/r1")

    //最终分区数量为: 👉 3个
    sc.stop()
  }
}
