package com.fanfan.tutoria.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月20日 09时00分
 * @Version: v1.0
 */
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
//                .setParallelism(1)
//                .shuffle()
//                .print("随机发送")
//                .setParallelism(2);

//        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
//                .setParallelism(1)
//                .rebalance()
//                .print("轮询发送")
//                .setParallelism(4);

        // fromElements 是 单并行度的 数据源
//        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
//                .setParallelism(1)
//                .broadcast()
//                .print("广播发送")
//                .setParallelism(2);

        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .setParallelism(1)
                .global()
                .print("global")
                .setParallelism(129);

        env.execute();
    }
}
