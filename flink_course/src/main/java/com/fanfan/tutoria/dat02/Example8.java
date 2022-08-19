package com.fanfan.tutoria.dat02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example8
 * @Description: TODO 练习物理分区算子
 * @Author: fanfan
 * @DateTime: 2022年08月19日 17时38分
 * @Version: v1.0
 */
public class Example8 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // shuffle
        env
                .fromElements(1,2,3,4,5,6,7,8,9)
                .shuffle()
                .print("shuffle 物理分区")
                .setParallelism(4);

        env
                .fromElements(1,2,3,4,5,6,7,8,9)
                .global()
                .print("global 物理分区")
                .setParallelism(4);

        env
                .fromElements(1,2,3,4,5,6,7,8,9)
                .broadcast()
                .print("broadcast 物理分区")
                .setParallelism(4);


        env
                .fromElements(1,2,3,4,5,6,7,8,9)
                .rebalance()
                .print("轮询 物理分区")
                .setParallelism(4);

        env.execute();
    }
}
