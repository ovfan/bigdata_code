package com.fanfan.flink.java.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example3
 * @Description: POJO 类模拟scala中的样例类
 * @Author: fanfan
 * @DateTime: 2022年06月14日 14时58分
 * @Version: v1.0
 */
public class Example3 {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements("hello world", "hello world").setParallelism(1)
                .flatMap(new Tokenizer()).setParallelism(1)
                .keyBy(r -> r.word)
                .reduce(new Sum())
                .setParallelism(1)
                .print()
                .setParallelism(1);

        env.execute();
    }

    public static class Sum implements ReduceFunction<WordCount>{

        @Override
        public WordCount reduce(WordCount in, WordCount accumulator) throws Exception {
            return new WordCount(in.word,in.count + accumulator.count);
        }
    }
    public static class Tokenizer implements FlatMapFunction<String,WordCount>{

        @Override
        public void flatMap(String in, Collector<WordCount> out) throws Exception {
            String[] words = in.split("\\s+");
            for (String word : words) {
                out.collect(new WordCount(word,1));
            }
        }
    }
    // POJO Class
    // 用来模拟scala的样例类：case class WordCount(word: String, count: Int)
    // 1. 必须是公有类
    // 2. 所有字段必须是公有字段
    // 3. 必须有空构造器
    public static class WordCount{
        public String word;
        public Integer count;

        public WordCount() {
        }

        public WordCount(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
