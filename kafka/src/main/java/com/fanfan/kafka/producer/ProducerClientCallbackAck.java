package com.fanfan.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

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
 * 需求：实现kafka生产者客户端，设置相关的ack应答机制完成异步发送
 */
public class ProducerClientCallbackAck {
    public static void main(String[] args) {
        // 1. 创建配置对象
        Properties properties = new Properties();
        // 1.1 配置kafka服务器连接地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.202.102:9092,192.168.202.103:9092");
        // 1.2 指定发送内容的key和value的序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 1.3 设置ack确认级别 all 等价于 -1
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        //     遇到发送错误设置重试次数为5次，不然生产者会一直重试
        properties.put(ProducerConfig.RETRIES_CONFIG, "5");
        //     开启幂等性（kafka默认就是开启的）
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // 2. 获取客户端连接对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // 3. 调用send方法，进行消息的发送
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("first", "hello fan"), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.err.println(metadata.topic() + "主题-" + metadata.partition() + "号分区，成功收到消息");
                    }
                }
            });
        }

        // 4. 关闭资源
        kafkaProducer.close();
    }
}
