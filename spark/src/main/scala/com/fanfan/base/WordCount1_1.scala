package com.fanfan.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount1_1 {
  def main(args: Array[String]): Unit = {
    // 1.准备spark运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(conf)

    // 2. 代码逻辑
    val lineDatas: RDD[String] = sc.textFile("spark/src/main/resources/word.txt")
    val wordDatas: RDD[String] = lineDatas.flatMap(_.split(" "))
    val wordGroups: RDD[(String, Iterable[String])] = wordDatas.groupBy(r => r)
    val wordCount: RDD[(String, Int)] = wordGroups.mapValues(_.size)
    wordCount.collect().foreach(println)

    // 3. 释放环境对象
    sc.stop()
  }

}
