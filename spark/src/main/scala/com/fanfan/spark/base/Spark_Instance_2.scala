package com.fanfan.spark.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Instance_2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 从磁盘中获取RDD
    // textFile可以指定文件路径，获取RDD对象,对磁盘文件中的数据进行处理
    val rdd: RDD[String] = sc.textFile("spark/src/main/resources/word.txt")
    rdd.collect().foreach(println)

    sc.stop()
  }
}
