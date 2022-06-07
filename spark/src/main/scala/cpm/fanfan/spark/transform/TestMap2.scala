package cpm.fanfan.spark.transform

import org.apache.spark.{SparkConf, SparkContext}

object TestMap2 {
  //将apache.log文件中，url一栏取出
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Map")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("spark/src/main/resources/apache.log",1)
    val newRDD = rdd.map(line => {
      val str: Array[String] = line.split(" ")
      str(6)
    })

    newRDD.saveAsTextFile("spark/src/main/resources/r4")
    sc.stop()
  }
}
