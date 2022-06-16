package com.fanfan.flink.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example1_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月16日 23时02分
 * @Version: v1.0
 */
public class Example1_1 {
    public static void main(String[] args) throws Exception {
        //TODO 复习第一天WordCount案例
        // 从数组元素中当成数据源
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("hello scala", "hello spark", "hello  scala")
                .flatMap(new Tokenizer())
                .keyBy("f0")
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> in, Tuple2<String, Integer> acc) throws Exception {
                        return Tuple2.of(in.f0, in.f1 + acc.f1);
                    }
                })
                .print();


        env.execute();
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = in.split("\\s+");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }
    }
}
