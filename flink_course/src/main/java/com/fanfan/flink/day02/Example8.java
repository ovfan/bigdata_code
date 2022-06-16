package com.fanfan.flink.day02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example8
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月16日 20时03分
 * @Version: v1.0
 */
public class Example8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1, 2, 3)
                // 1 -> (1,1)
                .map(r -> Tuple2.of(r, r))
                // 泛型在编译期会被类型擦除 Tuple<Integer, Integer> -> Tuple<Object, Object>
                .returns(Types.TUPLE(
                        Types.INT,
                        Types.INT
                ))
                .print("注意泛型擦除的使用规则");

        env.execute();
    }
}
