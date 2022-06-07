package cpm.fanfan.spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Map")
    val sc = new SparkContext(conf)
    val rdd:RDD[Int] = sc.makeRDD(List(1,2,3,4,5),3)
    // 需求：将现有的数据集的每一个元素乘以2后返回
    // map算子只需要传递一个参数即可。这个参数为函数类型： Int => U

    val newRDD: RDD[Int] = rdd.map(num => {
      num * 2  // num.toString 也可以转换数据类型
    })

    // 将分区内的文件保存为文本文件
    rdd.saveAsTextFile("output/o1")
    newRDD.saveAsTextFile("output/o2")

    sc.stop()
  }
}
