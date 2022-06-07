package com.fanfan.gmall.realtime.app

import java.lang

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.fanfan.gmall.realtime.bean.PageLog
import com.fanfan.gmall.realtime.utilwang.MyKafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
 * 日志数据消费分流
 */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    // 1. 准备实时环境 (每5s拉一批数据)
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 2.从kafka中消费数据
    val topic: String = "ODS_BASE_LOG"
    val groupId: String = "ODS_BASE_LOG_GROUP"

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaDStream(ssc, topic, groupId)

    // 3. 转换结构，将ConsumerRecord对象中的value提取出来，转换成方便操作的格式
    //  通用结构：JsonObject  Map
    //  专用结构: Bean
    val jsonObjDStream = kafkaDStream.map(ConsumerRecord => {
      val msgValue: String = ConsumerRecord.value()
      // 转换成Json对象
      val jsonObject = JSON.parseObject(msgValue)
      jsonObject
    })
    // 打印100行数据 测试用：
    // jsonObjDStream.print(100)

    // 4. 分流
    // 分流原则:
    //  1. 错误数据, 只要包含err的数据都定义为错误数据，错误数据不进行分流,直接完整的写入到错误主题中
    //  2 . 页面访问数据
    //    2.1 page数据分流到页面访问topic
    //    2.2 display数据分流到曝光topic
    //    2.3 action数据分流到动作topic
    //  3. 启动数据分流到启动topic
    val error_topic = "ERROR_TOPIC"
    val page_topic = "PAGE_TOPIC"
    val display_topic = "DISPLAY_TOPIC"
    val action_topic = "ACTION_TOPIC"
    val start_topic = "START_TOPIC"

    jsonObjDStream.foreachRDD(
      rdd => {
        rdd.foreach(
          jsonObj => {
            // 每条数据的分流处理
            // 分流错误数据
            val errObj: JSONObject = jsonObj.getJSONObject("err")
            if (errObj != null) {
              // 生产消息
              MyKafkaUtils.send(error_topic, jsonObj.toJSONString)
            } else {
              // 提取公共字段common
              val commonObj = jsonObj.getJSONObject("common")
              val ar: String = commonObj.getString("ar")
              val uid: String = commonObj.getString("uid")
              val os: String = commonObj.getString("os")
              val ch: String = commonObj.getString("ch")
              val isNew: String = commonObj.getString("is_new")
              val md: String = commonObj.getString("md")
              val mid: String = commonObj.getString("mid")
              val vc: String = commonObj.getString("vc")
              val ba: String = commonObj.getString("ba")

              // 提取公共字段 ts
              val ts: lang.Long = jsonObj.getLong("ts")

              // 分流页面访问数据
              val pageObj: JSONObject = jsonObj.getJSONObject("page")
              if (pageObj != null) {
                // 提取页面字段
                val pageId: String = pageObj.getString("page_id")
                val lastPageId: String = pageObj.getString("last_page_id")
                val pageItem: String = pageObj.getString("item")
                val pageItemType: String = pageObj.getString("item_type")
                val duringTime: lang.Long = pageObj.getLong("during_time")
                val sourceType: String = pageObj.getString("source_type")

                // 将字段封装到PageLog中
                val pageLog: PageLog = PageLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, ts)

                // 将pageLog转换成json字符串
                val pageLogJson: String = JSON.toJSONString(pageLog, new SerializeConfig(true))
                // 将页面访问数据写入到相应的主题中
                MyKafkaUtils.send(page_topic, pageLogJson)
              }

              // 分流启动数据
              val startObj: JSONObject = jsonObj.getJSONObject("start")
              if (startObj != null) {

              }
            }
          }
        )
      }
    )

    ssc.start()
    ssc.awaitTermination()

  }
}
