package com.fanfan.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @ClassName: ConsumerClient
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月03日 19时10分
 * @Version: v1.0
 */
public class ConsumerClient3 {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.202.102:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        // 设置消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test1");
        // 创建客户端连接对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        ArrayList<String> topicList = new ArrayList<>();
        topicList.add("first");

        // 订阅主题
        kafkaConsumer.subscribe(topicList);
        while (true){
            // 拉取消息
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofMillis(2));
            for (ConsumerRecord<String, String> consumerRecord : poll) {
                System.out.println(consumerRecord);
            }
        }
    }
}
