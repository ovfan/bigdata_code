package com.fanfan.tutoria.day07;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @ClassName: Example1
 * @Description: TODO sink to kafka
 * @Author: fanfan
 * @DateTime: 2022年08月26日 08时54分
 * @Version: v1.0
 */
public class Example1 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.put("bootstrap.servers","hadoop102:9092");

        env
                .readTextFile("flink_course/src/main/resources/UserBehavior.csv")
                .addSink(new FlinkKafkaProducer<String>(
                        "topic-userbehavior-0321",
                        new SimpleStringSchema(),
                        properties
                ));

        env.execute();
    }
}
