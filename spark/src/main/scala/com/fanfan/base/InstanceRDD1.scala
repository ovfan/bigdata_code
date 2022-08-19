package com.fanfan.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object InstanceRDD1 {
  def main(args: Array[String]): Unit = {
    // 准备Spark环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Instance")
    val sc: SparkContext = new SparkContext(conf)

    // 构建RDD的几种方式
    // 1. 从内存中构建RDD
    val seq: Seq[Int] = Seq(1, 2, 3, 4)
    val rdd1: RDD[Int] = sc.parallelize(seq)
    val rdd2: RDD[Int] = sc.makeRDD(seq)

    // 2. 从磁盘中获取RDD
    val rdd3: RDD[String] = sc.textFile("spark/src/main/resources/apache.log")
    rdd3.collect().foreach(println)
    rdd2.collect().foreach(println)
  }
}
