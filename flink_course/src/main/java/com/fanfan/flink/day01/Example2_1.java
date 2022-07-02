package com.fanfan.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: Example2_1
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年07月02日 23时46分
 * @Version: v1.0
 */
public class Example2_1 {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .readTextFile("flink_course/src/main/resources/word.txt")
                .setParallelism(1)
                .flatMap(new Tokenizer())
                .setParallelism(1)
                .keyBy(r -> r.word)
                .reduce(new Sum())
                .setParallelism(1)
                .print()
                .setParallelism(1);

        env.execute();
    }
    public static class Sum implements ReduceFunction<WordCount>{
        @Override
        public WordCount reduce(WordCount acc, WordCount in) throws Exception {
            return new WordCount(in.word,acc.count + in.count);
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
