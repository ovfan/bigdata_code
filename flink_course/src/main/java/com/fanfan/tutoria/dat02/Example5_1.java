package com.fanfan.tutoria.dat02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example5_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月19日 16时38分
 * @Version: v1.0
 */
public class Example5_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置全局并行度
        env.setParallelism(1);

        env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8,9)
                .keyBy(r -> r % 4)
                .reduce((v1, v2) -> v1 + v2)
                .print()
                .setParallelism(4);
        env.execute();
    }
}
