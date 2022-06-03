package com.fanfan.kafka.consumer;

import com.fanfan.kafka.producer.MyPartitioner;
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
 * 需求：实现kafka生产者客户端，完成异步发送
 */
public class ProducerClient {
    public static void main(String[] args) throws InterruptedException {
        // 1. 创建配置对象
        Properties properties = new Properties();
        // 1.1 配置kafka服务器连接地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.202.102:9092,192.168.202.103:9092");
        // 1.2 指定发送内容的key和value的序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.LINGER_MS_CONFIG,3);

        // 2. 获取客户端连接对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // 3. 调用send方法，进行消息的发送
        for (int i = 0; i < 100; i++) {
            kafkaProducer.send(new ProducerRecord<String, String>("first", "hello klk" + i), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.err.println(metadata.topic() + "主题-" + metadata.partition() + "号分区，成功收到消息");
                    }
                }
            });
        }
        Thread.sleep(2);

        // 4. 关闭资源
        kafkaProducer.close();
    }
}
