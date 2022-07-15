package com.fanfan.flink.day03_2;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月15日 15时00分
 * @Version: v1.0
 */
public class Example1 {
    public static void main(String[] args) throws Exception {
        //TODO review the physical partition
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // data source
//        env
//                .fromElements(1,2,3,4,5,6,7,8,9)
//                .shuffle()
//                .print("random send")
//                .setParallelism(4);

        // global
//        env
//                .fromElements(1,2,3,4,5,6,7,8,9)
//                .global()
//                .print("partition one")
//                .setParallelism(4);
//        env
//                .fromElements(1,2,3,4,5,6,7,8,9)
//                .broadcast()
//                .print("broadcast")
//                .setParallelism(4);
        env
                .fromElements(1,2,3,4,5,6,7,8,9)
                .rebalance()
                .print("轮询发送")
                .setParallelism(4);

        env.execute();
    }
}
