package com.fanfan.kafka.consumer.commitOffset;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @ClassName: ConsumerCommitSync
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月04日 00时09分
 * @Version: v1.0
 *
 * *  * 消费者客户端
 *  *  * 1. 获取客户端连接对象
 *  *  * 2. 订阅主题
 *  *  * 3. 调用API方进行消费
 *  *  * 4. 数据的处理（根据业务需求而定....）--实时开发中用到的多
 */
public class ConsumerCommitSync {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // 1. kafka消费者端 配置对象需要配置连接地址，解序列化器，关闭自动提交，设置消费者组
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.202.102:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);

        // 关闭offset自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        // 设定消费者组标识
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test1");

        // 2. 创建kafka 消费者端对象用于消费数据
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        // 2.1 消费者需要订阅主题消费
        ArrayList<String> topicList = new ArrayList<>();
        topicList.add("first");
        kafkaConsumer.subscribe(topicList);

        // 3. 消费消费

        while(true){
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(20));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println(record);
            }

            // 手动同步提交
            kafkaConsumer.commitSync();

            // 异步提交：
            // kafkaConsumer.commitAsync();
        }
    }
}
