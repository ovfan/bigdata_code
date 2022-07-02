package com.fanfan.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example3
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月02日 23时51分
 * @Version: v1.0
 * TODO 从数组中获取元素
 */
public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .fromElements("hello world", "hello world")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = in.split("\\s+");

                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .keyBy(r -> r.f0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> acc, Tuple2<String, Integer> in) throws Exception {
                        return Tuple2.of(in.f0, acc.f1 + in.f1);
                    }
                })
                .print();


        // 提交并执行任务
        env.execute();
    }
}
