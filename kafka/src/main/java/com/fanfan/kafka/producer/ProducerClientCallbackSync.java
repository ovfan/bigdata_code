package com.fanfan.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @ClassName: ProducerClient
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月02日 22时33分
 * @Version: v1.0
 * 面向客户端开发步骤：
 * 1. 获取客户端连接对象
 * 2. 根据API方法完成具体功能开发
 * 3. 关闭资源
 * <p>
 * 需求：实现kafka生产者客户端，完成同步发送
 *  同步发送就是在send后加上get()即可
 */
public class ProducerClientCallbackSync {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 1. 创建配置对象
        Properties properties = new Properties();
        // 1.1 配置kafka服务器连接地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.202.102:9092,192.168.202.103:9092");
        // 1.2 指定发送内容的key和value的序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 2. 获取客户端连接对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // 3. 调用send方法，进行消息的发送
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("first", "hello fan" + i * 2), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.err.println("主题： " + metadata.topic() + "  分区： " + metadata.partition());
                    }
                }
            }).get();
        }

        // 4. 关闭资源
        kafkaProducer.close();
    }
}
