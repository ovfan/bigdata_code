package com.fanfan.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月02日 22时49分
 * @Version: v1.0
 * TODO 实现网络端口来的数据 的 词频统计
 */
public class Example1 {
    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                // 数据源是socket
                // 先启动nc -lk 9999
                .socketTextStream("hadoop102", 9999)
                .setParallelism(1)
                .flatMap(new Tokenizer())
                .setParallelism(1)
                .keyBy(r -> r.f0) //按照单词进行分组
                .reduce(new Sum())
                .setParallelism(1)
                // 将结果打印输出
                .print()
                .setParallelism(1);
        // 提交并执行程序
        env.execute();
    }



    /**
     * 分词器
     * FlatMapFunction<IN,OUT>
     */
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
