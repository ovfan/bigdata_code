package com.fanfan.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount2 {
  def main(args: Array[String]): Unit = {
    // 1. 准备spark运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)

    // 2. 利用reduceByKey完成 wordCount --> 升级版，性能较好
    // reduceByKey要求的数据结构为 K-V 键值对，所以需要将数据转换为 Tuple(word, 1)
    val lineDatas: RDD[String] = sc.textFile("spark/src/main/resources/word.txt")
    val wordDatas: RDD[String] = lineDatas.flatMap(_.split(" "))
    val wordToOne: RDD[(String, Int)] = wordDatas.map(word => (word, 1))
    val wordCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    // 2.1 打印计算结果
    wordCount.collect().foreach(println)

    // 释放环境对象
    sc.stop()
  }
}
