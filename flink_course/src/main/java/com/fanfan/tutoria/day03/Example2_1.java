package com.fanfan.tutoria.day03;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example2_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月20日 09时16分
 * @Version: v1.0
 */
public class Example2_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .setParallelism(1)
                .keyBy(new KeySelector<Integer, Integer>() {
                    // 用来指定输入数据的key
                    @Override
                    public Integer getKey(Integer in) throws Exception {
                        return in % 3;
                    }
                })
                .print()
                .setParallelism(4);

        env.execute();
    }
}
