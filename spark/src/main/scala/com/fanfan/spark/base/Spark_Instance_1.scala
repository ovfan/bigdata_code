package com.fanfan.spark.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Instance_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // RDD实例化的方式1，从内存中创建rdd对象
    // 将内存中的数据作为处理的数据源
    val seq: Seq[Int] = Seq(1, 2, 3, 4)

    val rdd: RDD[Int] = sc.parallelize(seq)
    val rdd1:RDD[Int] = sc.makeRDD(seq)

    rdd.collect().foreach(println)
    sc.stop()
  }
}
