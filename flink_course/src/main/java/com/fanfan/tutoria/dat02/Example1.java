package com.fanfan.tutoria.dat02;

import com.fanfan.flink.day03_1.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月19日 14时15分
 * @Version: v1.0
 */
public class Example1 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置全局并行度
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .print();

        env.execute();
    }
}
