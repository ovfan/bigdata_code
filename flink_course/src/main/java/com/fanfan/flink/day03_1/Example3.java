package com.fanfan.flink.day03_1;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example3
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月05日 15时17分
 * @Version: v1.0
 */
public class Example3 {
    //TODO 泛型擦除 练习
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .fromElements(1,2,3,4)
                .map(r -> Tuple2.of(r,r))
                .returns(Types.TUPLE(Types.INT,Types.INT))
                .print();

        env.execute();
    }

}
