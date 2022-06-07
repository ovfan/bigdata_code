package com.fanfan.spark.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Parallelism_1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test").setMaster("local[*]")
    // 配置对象中设置分区数量
    conf.set("spark.default.parallelism", "6")

    val sc = new SparkContext(conf)

    // 磁盘文件--分区数量
    // minpartitions = 3
    val rdd1 = sc.textFile("spark/src/main/resources/num.txt", 3)

    // 分区数量计算:
    // 理想状态下: goalsize = 7 / 3 =2 ,每个分区放入两个字节
    // totalsize / goalsize = 3....1  33% > 10% 所以会再创建一个分区，3 + 1 =4

    // 验证：
    rdd1.saveAsTextFile("spark/src/main/resources/r3")
    //最终分区数量为: 👉 4个
    sc.stop()
  }
}
