package com.fanfan.tutoria.dat02;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月19日 09时07分
 * @Version: v1.0
 */
public class ReduceExer1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置全局并行度
        env.setParallelism(1);

        // 从数组中读取数据
        env
                .fromElements("hello.world", "hello.fanfan")
                .flatMap((String in, Collector<Tuple2<String,Integer>> out) -> {
                    for (String word : in.split("\\.")) {
                        out.collect(Tuple2.of(word,1));
                    }
                })
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> in) throws Exception {
                        return in.f0;
                    }
                })
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> acc, Tuple2<String, Integer> in) throws Exception {
                        System.out.println("acc.f0 = " + acc.f0);
                        return Tuple2.of(in.f0,acc.f1+in.f1);
                    }
                })
                .print();

        env.execute();
    }
}
