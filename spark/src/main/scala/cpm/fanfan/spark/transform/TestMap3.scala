package cpm.fanfan.spark.transform

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object TestMap3 {
  def main(args: Array[String]): Unit = {
    // 测试映射
    // str = "hello fan fan hi"
    val str = "hello fan fan hi"
    val list = List("hello","fan","fan","hi")
    val newList = list.map(str => {
      val listBuffer = ListBuffer[String]()
      str + "!"
    })
    println(newList)

  }
}
