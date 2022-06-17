package com.fanfan.flink.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月17日 09时39分
 * @Version: v1.0
 * TODO 练习物理分区算子 shuffle rebalancer global  broadcast(广播)
 */
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .setParallelism(1)
                .shuffle()
                .print("使用shuffle将数据随机发送到print的并行子任务中执行")
                .setParallelism(4);

        env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .setParallelism(1)
                .rebalance()
                .print("使用rebalance将数据轮询发送到print的并行子任务中执行")
                .setParallelism(4);
        env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .setParallelism(1)
                .global()
                .print("使用global将数据发送的print的第一个并行子任务线程中执行")
                .setParallelism(4);
        env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .setParallelism(1)
                .broadcast()
                .print("使用broadcast广播发送 将数据广播到print每一个并行子任务中执行")

                .setParallelism(4);


        env.execute();
    }
}
