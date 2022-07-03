package com.fanfan.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example3_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月03日 09时59分
 * @Version: v1.0
 * TODO 使用POJO类完成wordcount词频统计
 */
public class Example3_1 {
    public static void main(String[] args) throws Exception{
        // 1. 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 添加数据源
        env
                .fromElements("hello  world","hello world")
                .flatMap(new Tokenizer())
                .keyBy(r -> r.word)
                .reduce(new ReduceFunction<WorldCount>() {
                    @Override
                    public WorldCount reduce(WorldCount acc, WorldCount in) throws Exception {
                        return new WorldCount(in.word,acc.count+in.count);
                    }
                })
                .print();

        env.execute();
    }
    public static class Tokenizer implements FlatMapFunction<String,WorldCount>{
        @Override
        public void flatMap(String in, Collector<WorldCount> out) throws Exception {
            String[] words = in.split("\\s+");
            for (String word : words) {
                out.collect(new WorldCount(word,1));
            }
        }
    }
    public static class WorldCount{
        public String word;
        public Integer count;

        public WorldCount() {
        }

        public WorldCount(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WorldCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
