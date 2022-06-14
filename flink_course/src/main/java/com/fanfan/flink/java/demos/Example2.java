package com.fanfan.flink.java.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example2
 * @Description: 从文本读取文件,计算wordcount
 * @Author: fanfan
 * @DateTime: 2022年06月14日 14时47分
 * @Version: v1.0
 */
public class Example2 {
    public static void main(String[] args) throws Exception{
        // 获取流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从文本读取数据
        env.readTextFile("D:\\gitProjects\\bigdata_code\\flink_course\\src\\main\\resources\\word.txt")
                .flatMap(new Tokenizer())
                .keyBy(tuple -> tuple.f0)
                .reduce(new Sum())
                .print()
                .setParallelism(1);

        env.execute();
    }
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String,Integer>> {

        @Override
        public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = in.split("\\s+");
            for (String word : words) {
                out.collect(Tuple2.of(word,1));
            }
        }
    }
    public static class Sum implements ReduceFunction<Tuple2<String,Integer>>{

        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> in, Tuple2<String, Integer> accumulator) throws Exception {
            return Tuple2.of(in.f0,in.f1 + accumulator.f1);
        }
    }
}
