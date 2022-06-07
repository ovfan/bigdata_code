package cpm.fanfan.spark.transform

import org.apache.spark.{SparkConf, SparkContext}

object TestFlatMap2 {
  def main(args: Array[String]): Unit = {
    // ✨练习：将List(List(1,2),3,List(4,5))进行扁平化操作
    val conf = new SparkConf().setMaster("local[*]").setAppName("flatMapDemo")
    val sc = new SparkContext(conf)
    val list = List(List(1,2),3,List(4,5))
    val rdd = sc.makeRDD(list)
    val newRDD = rdd.map {
      case list: List[_] => list
      case num: Int => List(num)
    }
    newRDD.flatMap(num => num)

    sc.stop()
  }
}
