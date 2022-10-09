package com.fanfan.flink.day01;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;

import java.util.Properties;

/**
 * @ClassName: Example4
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年10月09日 16时34分
 * @Version: v1.0
 */
public class Example4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 设置连接kafka的配置
        Properties props = new Properties();
        // 配置连接kafka集群的地址和消费者组
        props.setProperty("bootstrap.servers", "hadoop102:9092");
        props.setProperty("group.id", "hellokafka");

        env
                .addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), props))
                .print();
        env.execute();
    }
}
