package cpm.fanfan.spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestMap1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Map")
    val sc = new SparkContext(conf)
    val rdd:RDD[Int] = sc.makeRDD(List(1,2,3,4,5),3)

    val newRDD: RDD[Int] = rdd.map(num => {
      println("****" + num)
      num * 2  // num.toString 也可以转换数据类型
    })

    val newRDD1 = newRDD.map(num => {
      println("@@@@@")
      num.toString + " --"
    })

    // RDD执行过程中，分区内的数据处理是有序的，分区间处理数据是无序的
    // collect()触发RDD的执行

    newRDD1.collect()


    sc.stop()
  }
}
