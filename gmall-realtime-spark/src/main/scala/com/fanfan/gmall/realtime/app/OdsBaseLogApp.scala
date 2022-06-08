package com.fanfan.gmall.realtime.app

import java.lang

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.fanfan.gmall.realtime.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
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
            // TODO 0.分流错误数据
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

              // TODO 1. 分流页面访问数据
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

                // TODO 3. 分流曝光数据
                val displayArr: JSONArray = jsonObj.getJSONArray("displays")
                if (displayArr != null && displayArr.size() > 0) {
                  for (i <- 0 until displayArr.size()) {
                    val displayObj: JSONObject = displayArr.getJSONObject(i)
                    // 提取曝光字段
                    val displayType: String = displayObj.getString("display_type")
                    val displayItem: String = displayObj.getString("item")
                    val displayItemType: String = displayObj.getString("item_type")
                    val posId: String = displayObj.getString("pos_id")
                    val order: String = displayObj.getString("order")

                    // 分装成PageDisplayLog
                    val pageDisplayLog: PageDisplayLog = PageDisplayLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, displayType, displayItem, displayItemType, order, posId, ts)

                    // 写入kafka
                    MyKafkaUtils.send(display_topic, JSON.toJSONString(pageDisplayLog, new SerializeConfig(true)))

                  }
                }

                // TODO 4. 分流事件数据
                val actionArr: JSONArray = jsonObj.getJSONArray("actions")
                if (actionArr != null && actionArr.size() > 0) {
                  for(i <- 0 until actionArr.size()){
                    val actionObj: JSONObject = actionArr.getJSONObject(i)
                    // 提取事件字段
                    val actionItem: String = actionObj.getString("item")
                    val actionItemType: String = actionObj.getString("item_type")
                    val actionId: String = actionObj.getString("action_id")
                    val actionTs: Long = actionObj.getLong("ts")

                    // 封装事件Log对象：PageActionLog
                    val pageActionLog: PageActionLog = PageActionLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, actionId, actionItem, actionItemType, actionTs, ts)
                    // 发送到Kafka
                    MyKafkaUtils.send(action_topic,JSON.toJSONString(pageActionLog,new SerializeConfig(true)))

                  }
                }
              }

              // TODO 2.分流启动数据
              val startObj: JSONObject = jsonObj.getJSONObject("start")
              if (startObj != null) {
                val startObj: JSONObject = jsonObj.getJSONObject("start")
                if (startObj != null) {
                  // 提取启动字段
                  val entry: String = startObj.getString("entry") //入口
                  val openAdId: String = startObj.getString("open_ad_id") //开屏广告ID
                  val loadingTime: lang.Long = startObj.getLong("loading_time")
                  val openAdMs: lang.Long = startObj.getLong("open_ad_ms")
                  val openAdSkipMs: Long = startObj.getLong("open_ad_skip_ms")

                  // 封装成StartLog
                  val startLog: StartLog = StartLog(mid, uid, ar, ch, isNew, md, os, vc, ba, entry, openAdId, loadingTime, openAdMs, openAdSkipMs, ts)
                  // 发送到Kafka   -- 日志发送到kafka需要经过 fastjson的JSON工具类包装下
                  MyKafkaUtils.send(start_topic, JSON.toJSONString(startLog, new SerializeConfig(true)))
                }
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
