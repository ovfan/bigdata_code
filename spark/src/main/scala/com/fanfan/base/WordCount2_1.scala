package com.fanfan.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount2_1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount-reduce版")
    val sc: SparkContext = new SparkContext(conf)
    val lineDatas: RDD[String] = sc.textFile("spark/src/main/resources/word.txt")
    val wordDatas: RDD[String] = lineDatas.flatMap(_.split(" "))
    val wordToOne: RDD[(String, Int)] = wordDatas.map(r => (r, 1))

    val wordCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    wordCount.collect().foreach(println)


    sc.stop()
  }

}
