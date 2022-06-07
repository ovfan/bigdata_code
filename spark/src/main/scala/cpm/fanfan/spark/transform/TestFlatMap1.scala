package cpm.fanfan.spark.transform

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

// 将处理的数据进行扁平化后 再进行映射处理，所以算子也称之为扁平映射
// flatMap扁平化操作，1 => N 每一个个体都应该使用
object TestFlatMap1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("flatMapDemo")
    val sc = new SparkContext(conf)
    // 将一个一个的字符串，拆分成一个一个的单词
    // List("hello world", "hello scala", "hello spark")
    val rdd = sc.makeRDD(List("hello world", "hello scala", "hello spark"))

    // flatMap自定义的扁平化操作
    // 将处理的数据进行扁平化后再进行映射处理，所以算子也称之为扁平映射
    val newRDD = rdd.flatMap(str => {
      val word: Array[String] = str.split(" ")
      word
    })
    newRDD.foreach(println)
    sc.stop()
  }
}
