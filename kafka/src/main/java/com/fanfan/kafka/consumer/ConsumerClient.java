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
 *  * 消费者客户端
 *  * 1. 获取客户端连接对象
 *  * 2. 订阅主题
 *  * 3. 调用API方进行消费
 *  * 4. 数据的处理（根据业务需求而定....）
 */
public class ConsumerClient {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.202.102:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        // 设置消费者组标识
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
        // 创建客户端连接对象
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // 创建消费者订阅主题列表 -- 因为消费者可消费多个主题的数据
        ArrayList<String> topicList = new ArrayList<>();
        topicList.add("first");

        // 订阅主题
        kafkaConsumer.subscribe(topicList);
        while (true){
            // 每个2毫秒拉取一次消息
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofMillis(2));
            for (ConsumerRecord<String, String> consumerRecord : poll) {
                System.out.println(consumerRecord);
            }
        }
    }
}
