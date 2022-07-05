package com.fanfan.flink.day03_1;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: Example3_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月05日 15时24分
 * @Version: v1.0
 */
public class Example3_1 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new ClickSource())
                .map(r -> Tuple2.of(r.user,r.url))
                .returns(Types.TUPLE(
                        Types.STRING,
                        Types.STRING
                ))
                .print();

        env.execute();
    }
}
