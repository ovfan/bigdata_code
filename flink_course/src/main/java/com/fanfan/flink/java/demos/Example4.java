package com.fanfan.flink.java.demos;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example4
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月15日 15时40分
 * @Version: v1.0
 */
public class Example4 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env.execute();
    }
}
