package com.fanfan.tutoria.dat02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: ReduceExer2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月19日 10时46分
 * @Version: v1.0
 */
public class ReduceExer2 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从数组中读取元素
        env
                .fromElements("hello.world","hello.fanfan")
                .flatMap((String in, Collector<Tuple2<String,Integer>> out) -> {
                    String[] words = in.split("\\.");
                    for (String word : words) {
                        out.collect(Tuple2.of(word,1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(r -> r.f0)
                .reduce((v1,v2) -> Tuple2.of(v1.f0,v2.f1 + v1.f1))
                .print();

        env.execute();
    }
}
