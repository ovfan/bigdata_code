package com.fanfan.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount1 {
  def main(args: Array[String]): Unit = {
    // TODO 构建spark环境，完成WordCount功能
    // 1. 提前构建spark环境配置对象
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    // 1.1 构建spark环境对象
    val sc = new SparkContext(conf)

    // 2. 使用spark框架读取本地文件，进行分布式计算处理
    // 读取文件时，都是一行一行读取
    val lineDatas: RDD[String] = sc.textFile("spark/src/main/resources/word.txt")

    // 3. 实现WordCount计算逻辑
    // 3.1 将文件中的一行一行数据拆分成一个一个的单词
    //      line => word, word, word
    val wordDatas: RDD[String] = lineDatas.flatMap(_.split(" "))
    // 3.2 数据按照单词进行分组
    val wordGroups: RDD[(String, Iterable[String])] = wordDatas.groupBy(word => word)
    // 3.3 将分组后的数据进行统计分析
    val wordCount: RDD[(String, Int)] = wordGroups.mapValues(_.size)
    // 3.4 将统计的结果采集后打印在控制台上
    wordCount.collect().foreach(println)

    // 4. 执行完计算后，需要将环境对象释放掉
    sc.stop();
  }
}
