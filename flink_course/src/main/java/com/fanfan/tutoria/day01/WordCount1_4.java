package com.fanfan.tutoria.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: WordCount1_4
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年08月17日 15时09分
 * @Version: v1.0
 */
public class WordCount1_4 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.readTextFile("flink_course/src/main/resources/color.txt")
                .setParallelism(1)
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String in, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] words = in.split("\\s+");
                        for (String word : words) {
                            out.collect(Tuple2.of(word,1L));
                        }
                    }
                }).setParallelism(1)
                // r : 输入数据的字段
                .keyBy(r -> r.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> acc, Tuple2<String, Long> in) throws Exception {
                        return Tuple2.of(in.f0,in.f1 + acc.f1);
                    }
                }).setParallelism(1)
                .print()
                .setParallelism(1);

        env.execute();
    }
}
