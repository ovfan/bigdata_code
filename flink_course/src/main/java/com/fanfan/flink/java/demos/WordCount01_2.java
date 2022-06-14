package com.fanfan.flink.java.demos;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: WordCount01_2
 * @Description:
 * @Author: fanfan
 * @DateTime: 2022年06月15日 00时17分
 * @Version: v1.0
 */
public class WordCount01_2 {
    public static void main(String[] args) throws Exception {
        //需求:从文本获取输入流
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.readTextFile("flink_course/src/main/resources/word.txt")
                .flatMap(new Tokenizer())
                .keyBy(wordcount -> wordcount.word)
                .reduce(new Sum())
                .print();

        env.execute();

    }
    // 定义聚合逻辑的实现类
    private static class Sum implements ReduceFunction<WordCount>{

        @Override
        public WordCount reduce(WordCount in, WordCount acc) throws Exception {
            return new WordCount(in.word,in.count + acc.count);
        }
    }


    // 利用POJO 类来模拟Scala中样例类
    public static class WordCount {
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

    private static class Tokenizer implements FlatMapFunction<String, WordCount> {
        @Override
        public void flatMap(String in, Collector<WordCount> out) throws Exception {
            String[] words = in.split("\\s+");
            for (String word : words) {
                out.collect(new WordCount(word, 1));
            }
        }
    }
}
