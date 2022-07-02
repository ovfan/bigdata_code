package com.fanfan.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月02日 23时32分
 * @Version: v1.0
 * TODO 数据源更换为文件
 * Flink对于离线文件，也是一行一行处理---> flink 流批统一
 */
public class Example2 {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                // 数据源是文件
                .readTextFile("flink_course/src/main/resources/word.txt")
                .setParallelism(1)
                .flatMap(new Example1.Tokenizer())
                .setParallelism(1)
                .keyBy(r -> r.f0) //按照单词进行分组
                .reduce(new Example1.Sum())
                .setParallelism(1)
                // 将结果打印输出
                .print()
                .setParallelism(1);
        // 提交并执行程序
        env.execute();
    }

    // 分词器
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = in.split("\\s+");
            for (String word : words) {
                // out是一个集合，用来收集向下游发送的数据
                out.collect(Tuple2.of(word, 1));
            }
        }
    }

    // 统计计算
    public static class Sum implements ReduceFunction<Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> acc, Tuple2<String, Integer> in) throws Exception {
            return Tuple2.of(in.f0, acc.f1 + in.f1);
        }
    }
}
