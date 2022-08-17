package com.fanfan.tutoria.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: WordCount1
 * @Description: TODO 单词计数
 * @Author: fanfan
 * @DateTime: 2022年08月17日 11时22分
 * @Version: v1.0
 */
public class WordCount1 {
    // 不要忘记抛出异常!!!
    public static void main(String[] args) throws Exception {
        // 1. 获取flink 流执行环境(上下文)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 定义处理数据的有向无环图DAG
        // 从socket读取数据,先启动 `nc -lk 9999`
        env
                .socketTextStream("192.168.44.102", 9999)
                // 设置socketTextStream算子的并行子任务的数量 为 1
                .setParallelism(1)
                // map阶段 "hello world"  => (hello, 1) (world, 1)
                // 由于是 1对多的转换，所以使用flatMap算子
                .flatMap(new Tokenizer())
                // flatMap算子的并行子任务的数量是1
                .setParallelism(1)
                // shuffle阶段
                // 按照单词分组，将不同单词所对应的数据从逻辑上分开处理
                // KeySelector<输入数据的泛型,key的泛型>
                // keyBy不做任何计算工作，所以不能设置并行子任务的数量
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

                    @Override
                    public String getKey(Tuple2<String, Integer> in) throws Exception {
                        // 将元组的f0 字段指定为key
                        return in.f0;
                    }
                })
                // reduce阶段
                //
                .reduce(new WordCount())
                .setParallelism(1)
                .print()
                // print算子的并行子任务的数量为1
                .setParallelism(1);

        // 3.执行(提交并执行有向无环图 DAG)
        env.execute();
    }

    // FlatMapFunction<IN,OUT>
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 1. 使用空格切分字符串
            String[] words = in.split("\\s+");
            // 2. 将要发送的数据收集到集合中，由flink自动将数据发送出去
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }
    }

    // ReduceFunction<IN> 输入数据的泛型
    // 因为reduce的输入 输出 和 累加器的类型是一样的
    public static class WordCount implements ReduceFunction<Tuple2<String, Integer>> {
        // 两个参数，一个是累加器，一个是 输入数据
        // reduce方法定义的是：输入数据和累加器的聚合规则
        // 返回值 是 新的累加器
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> acc, Tuple2<String, Integer> in) throws Exception {
            return Tuple2.of(in.f0, in.f1 + acc.f1);
        }
    }

}

