package com.fanfan.gmall.realtime.utilwang

import com.fanfan.gmall.realtime.util.{MyConfigUtils, MyPropsUtils}
import org.apache.kafka.clients.consumer
import java.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
 * kafka工具类，用于消费和生产数据
 * 使用sparkstreaming消费数据
 */
object MyKafkaUtils {

  /**
   * 消费者配置
   *
   * 消费者配置类: ConsumerConfig
   */
  private val consumerParams: mutable.Map[String, Object] = mutable.Map[String, Object](
    //kafka集群位置
    // "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    // ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    // ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils("kafka.servers")
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils(MyConfigUtils.KAFKA_SERVERS),

    // Key 和 value的反序列化器
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> MyPropsUtils(MyConfigUtils.STRING_DESERIALIZER),

    // 消费者组
    // ConsumerConfig.GROUP_ID_CONFIG -> "test",

    // offset重置(重置策略 有latest ，earliest，none)
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    // 自动提交offset
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    // 提交频率（单位是毫秒） 每5秒自动提交一次offset
    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "5000"
  )

  /**
   * 从kafka中消费数据
   * 获取到InputDStream对象
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String): InputDStream[ConsumerRecord[String, String]] = {
    // 设置消费者组
    consumerParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    // 获取流
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerParams))

    inputDStream
  }


  /**
   * 生产者对象
   */
  private val producer: KafkaProducer[String, String] = createProducer()

  /**
   * 创建生产者对象
   *
   * 生产者配置类：ProducerConfig
   *
   * @return
   */
  def createProducer(): KafkaProducer[String, String] = {
    // 生产者配置对象
    val producerParams = new util.HashMap[String, AnyRef]()
    // kafka集群位置
    producerParams.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MyPropsUtils(MyConfigUtils.KAFKA_SERVERS))
    // key和value的序列化器
    producerParams.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, MyPropsUtils(MyConfigUtils.STRING_SERIALIZER))
    producerParams.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MyPropsUtils(MyConfigUtils.STRING_SERIALIZER))
    // acks
    // buffer.memory
    // retries
    // batch.size
    // linger.ms
    // enable.idempotence

    val producer = new KafkaProducer[String, String](producerParams)

    producer
  }

  /**
   * 往kafka中生产数据
   */
  def send(topic: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, msg))
  }
}
