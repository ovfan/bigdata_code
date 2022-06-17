package com.fanfan.flink.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example1_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 20时08分
 * @Version: v1.0
 * TODO 复习物理分区算子 shuffle rebalance rescale global broadcast
 */
public class Example1_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env
//                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
//                .setParallelism(1)
//                .shuffle()
//                .print()
//                .setParallelism(4);
//        env
//                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
//                .setParallelism(1)
//                .rebalance()
//                .print()
//                .setParallelism(4);
//        env.fromElements(1,2,3,4,5,6,7,8)
//                .setParallelism(1)
//                // 数据发送到第一个print并行子任务中执行
//                .global()
//                .print()
//                .setParallelism(4);
//        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
//                .setParallelism(1)
//                .broadcast()
//                .print()
//                .setParallelism(4);
        env.fromElements(1,2,3,4,5,6,7,8)
                .rescale()
                .print()
                .setParallelism(4);

        env.execute();
    }
}
