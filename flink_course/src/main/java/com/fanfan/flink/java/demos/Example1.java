package com.fanfan.flink.java.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @ClassName: Example1
 * @Description: 完成WordCount案例
 * @Author: fanfan
 * @DateTime: 2022年06月14日 12时20分
 * @Version: v1.0
 */
public class Example1 {
    // 记得抛出异常
    public static void main(String[] args) throws Exception {
        // 准备流处理的环境对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取输入的网络端口数据（Socket,9999）; 先启动"nc -lk 9999"
        env.socketTextStream("hadoop102", 9999)
                // 数据源的执行子任务并行度为1
                .setParallelism(1)
                // map阶段,"hello world" => (hello,1),(word,1)
                .flatMap(new Tokenizer())
                // flatMap的并行子任务的数量为1
                .setParallelism(1)
                // keyBy 按照相同的单词进行分组，此处涉及shuffle阶段
                // shuffle阶段没有并行子任务
                .keyBy(r -> r.f0)

                .reduce(new Sum())
                // reduce的并行子任务数量为1
                .setParallelism(1)
                // 打印输出结果
                .print()
                // print打印的并行子任务数量也为1
                .setParallelism(1);


        // 提交并执行任务
        env.execute();
    }

    // FlatMapFunction<IN,OUT>
    public static class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>> {
        // out是集合，用来收集向下游发送的数据
        @Override
        public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 使用空格切分字符串
            String[] words = in.split("\\s+");
            // 将单词转换为元组,并收集到集合中
            // flink会自动将集合中的数据向下游发送
            for(String word: words){
                out.collect(Tuple2.of(word, 1));
            }
        }
    }

    // reduce只有一个泛型
    // 因为reduce的输入、累加器、和输出的类型都是一样的
    public static class Sum implements ReduceFunction<Tuple2<String,Integer>>{

        // 定义输入数据的累加器的聚合规则
        // 返回值是新的累加器，会覆盖旧的累加器
        // reduce算子输出的是累加器的值
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> in, Tuple2<String, Integer> accumulator) throws Exception {

            return Tuple2.of(in.f0,in.f1 + accumulator.f1);
        }
    }
}

