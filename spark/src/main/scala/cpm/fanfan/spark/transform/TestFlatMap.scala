package cpm.fanfan.spark.transform

import org.apache.spark.{SparkConf, SparkContext}

// 将处理的数据进行扁平化后再进行映射处理，所以算子也称之为扁平映射
// flatMap扁平化操作，1 => N 每一个个体都应该使用
object TestFlatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("flatMapDemo")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(List(1,2),List(3,4),List(5,6)))

    val newRDD = rdd.flatMap(list => list)
    newRDD.collect().foreach(println)
    sc.stop()
  }
}
