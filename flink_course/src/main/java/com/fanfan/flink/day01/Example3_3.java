package com.fanfan.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example3_3
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月05日 10时44分
 * @Version: v1.0
 */
public class Example3_3 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //TODO 实现wordcount
        env
                .fromElements("hello world","hello world")
                .flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] words = s.split("\\s+");
                        for (String word : words) {
                            collector.collect(Tuple2.of(word,1));
                        }
                    }
                })
                .keyBy(r -> r.f0)
                .sum("f1")
                .print();

        env.execute();
    }
}
